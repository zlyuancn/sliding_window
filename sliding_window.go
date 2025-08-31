package sliding_window

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
)

var ErrIsStop = errors.New("is stop err")

type SlidingWindow struct {
	data        []bool        // 环形数据, 表示每条数据是否已处理
	dIndex      int           // 环形数据起始位置索引
	startDataSn int64         // 起始位置映射表示的数据sn
	space       chan struct{} // 滑动窗口可用窗口数量

	ackCh        chan int64 // ack数据通道, 用于并发转串行
	submitDataSn int64      // 已完成提交的数据sn

	waitProgressOk int64 // 等待达到指定进度
	waitCh         chan struct{}
	isCloseWaitCh  int32 // 防止重复关闭

	stopCh        chan struct{} // 停止通道
	isCloseStopCh int32         // 防止重复关闭
}

// 创建一个滑动窗口(窗口大小, 第一个要处理的数据sn)
func NewSlidingWindow(size int, startDataSn int64) *SlidingWindow {
	if size < 1 {
		panic("startDataSn must >= 1")
	}
	if startDataSn < 0 {
		panic("startDataSn must >= 0")
	}

	s := &SlidingWindow{
		data:        make([]bool, size),
		dIndex:      0,
		startDataSn: startDataSn,
		space:       make(chan struct{}, size),

		ackCh:        make(chan int64),
		submitDataSn: startDataSn - 1,

		waitProgressOk: -1,
		waitCh:         make(chan struct{}),

		stopCh: make(chan struct{}),
	}
	go s.ackLoop()
	return s
}

// 准备处理下一个数据sn, 如果滑动窗口空间已满, 会等待直到ack空出. return(下一个要处理的数据sn, err)
func (s *SlidingWindow) Next(ctx context.Context) (int64, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-s.stopCh:
		return 0, ErrIsStop
	case s.space <- struct{}{}: // 占用一个空间
	}

	sn := atomic.AddInt64(&s.submitDataSn, 1)
	return sn, nil
}

// 数据完成确认(数据sn)
func (s *SlidingWindow) Ack(dataSn int64) {
	if dataSn < 0 {
		panic("dataSn must >= 0")
	}
	select {
	case s.ackCh <- dataSn:
	case <-s.stopCh:
	}
}

func (s *SlidingWindow) ackLoop() {
	for {
		select {
		case dataSn := <-s.ackCh:
			ssn := atomic.LoadInt64(&s.startDataSn)
			if dataSn < ssn {
				fmt.Printf("[ERROR] ack dataSn(%d) must >= s.startDataSn(%d)", dataSn, ssn)
				continue
			}
			if dataSn > ssn+int64(len(s.data)-1) {
				fmt.Printf("[ERROR] ack dataSn(%d) must <= now space maxDataSn(%d)", dataSn, ssn+int64(len(s.data)-1))
				continue
			}

			s.data[s.getIndex(ssn, dataSn)] = true

			// 只有第一个数据完成才会开始滑动窗口
			if dataSn == ssn {
				ssn = s.slide()
				s.checkWaitProgress(ssn - 1)
			}
		case <-s.stopCh:
			return
		}
	}
}

// 等待达到指定进度, 传入一个数据sn, 表示 dataSn 及之前的数据都已处理完成
func (s *SlidingWindow) Wait(ctx context.Context, dataSn int64) (err error) {
	if dataSn < 0 {
		return errors.New("dataSn must >= 0")
	}

	if !atomic.CompareAndSwapInt64(&s.waitProgressOk, -1, dataSn) { // 首次使用
		if !atomic.CompareAndSwapInt64(&s.waitProgressOk, dataSn, dataSn) { // 多次使用
			return errors.New("Repeat the Wait operation, but with different dataSn data.")
		}
	}

	if s.GetProgress() >= s.waitProgressOk {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stopCh:
		return ErrIsStop
	case <-s.waitCh:
	}

	return nil
}

func (s *SlidingWindow) Stop() {
	if atomic.AddInt32(&s.isCloseStopCh, 1) == 1 {
		close(s.stopCh)
	}
}

// 获取数据sn在环形数据中的索引位置
func (s *SlidingWindow) getIndex(startDataSn, dataSn int64) int {
	n := dataSn - startDataSn  // 相对起始数据sn偏移值
	index := s.dIndex + int(n) // 索引位置=起始索引位置+偏移值
	if index >= len(s.data) {  // 转完一圈
		index -= len(s.data)
	}
	return index
}

// 滑动. return (最新的 startDataSn)
func (s *SlidingWindow) slide() int64 {
	n := 0                 // 环指针移动了多少次
	for s.data[s.dIndex] { // 如果当前位置已完成
		s.data[s.dIndex] = false // 把当前位置置空
		// 移动环指针
		s.dIndex++                   // 索引偏移
		if s.dIndex == len(s.data) { // 绕完一圈
			s.dIndex = 0
		}
		n++
	}
	ssn := atomic.AddInt64(&s.startDataSn, int64(n)) // 数据sn要跟着一起偏移
	// 滑动窗口后释放了n个空间
	for i := 0; i < n; i++ {
		<-s.space // 这里是不可能阻塞的. 因为使用者首先占用了一个空间, 然后获取到的数据sn, 接着ack该数据sn之后才会释放这个空间
	}
	return ssn
}

// 获取进度, 返回一个数据sn, 表示这个数据sn以及其之前的数据都已经处理完成
func (s *SlidingWindow) GetProgress() int64 {
	return atomic.LoadInt64(&s.startDataSn) - 1
}

// 检查等待进度
func (s *SlidingWindow) checkWaitProgress(progress int64) {
	waitProgress := atomic.LoadInt64(&s.waitProgressOk)
	if waitProgress != -1 && progress >= waitProgress {
		s.closeWaitOkCh()
	}
}

func (s *SlidingWindow) closeWaitOkCh() {
	if atomic.AddInt32(&s.isCloseWaitCh, 1) == 1 {
		close(s.waitCh)
	}
}
