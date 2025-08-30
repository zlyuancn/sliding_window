package sliding_window

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestSlidingWindow(t *testing.T) {
	data := make([]int, 1000) // 待处理数据
	maxDataSn := int64(999)   // 最大数据sn

	windowSize := 100         // 滑动窗口大小
	startDataSn := int64(300) // 从这个数据sn开始处理

	// 创建一个滑动窗口
	sw := NewSlidingWindow(windowSize, startDataSn)
	defer sw.Stop()

	for {
		sn, err := sw.Next(context.Background())
		if err != nil {
			t.Fatalf("next err: %v", err)
		}

		// 模拟异步提交处理
		go func() {
			// 模拟io延迟
			time.Sleep(time.Millisecond * time.Duration(rand.Int31n(50)))
			// 数据处理
			data[sn] = 1

			// 告知处理完成
			sw.Ack(sn)
		}()

		// 所有数据都提交处理了
		if sn == maxDataSn {
			t.Logf("submit ok")
			break
		}
	}

	t.Logf("wait process")

	// 等待完成
	err := sw.Wait(context.Background(), maxDataSn)
	if err != nil {
		t.Fatalf("wait err: %v", err)
	}

	// 检查数据
	for i := startDataSn; i <= maxDataSn; i++ {
		if data[i] != 1 {
			t.Fatalf("data[%d] != 1. %+v", i, data)
		}
	}
	t.Logf("process ok")
}

func TestSlidingWindowAndRate(t *testing.T) {
	data := make([]int, 1000) // 待处理数据
	maxDataSn := int64(999)   // 最大数据sn

	windowSize := 100         // 滑动窗口大小
	startDataSn := int64(300) // 从这个数据sn开始处理

	limiter := rate.NewLimiter(300, 1) // 每秒限速

	// 创建一个滑动窗口
	sw := NewSlidingWindow(windowSize, startDataSn)
	defer sw.Stop()

	for {
		sn, err := sw.Next(context.Background())
		if err != nil {
			t.Fatalf("next err: %v", err)
		}

		// 获取限速令牌
		err = limiter.Wait(context.Background())
		if err != nil {
			t.Fatalf("limiter.Wait err: %v", err)
		}

		// 模拟异步提交处理
		go func() {
			// 模拟io延迟
			time.Sleep(time.Millisecond * time.Duration(rand.Int31n(50)))
			// 数据处理
			data[sn] = 1

			// 告知处理完成
			sw.Ack(sn)
		}()

		// 所有数据都提交处理了
		if sn == maxDataSn {
			t.Logf("submit ok")
			break
		}
	}

	t.Logf("wait process")

	// 等待完成
	err := sw.Wait(context.Background(), maxDataSn)
	if err != nil {
		t.Fatalf("wait err: %v", err)
	}

	// 检查数据
	for i := startDataSn; i <= maxDataSn; i++ {
		if data[i] != 1 {
			t.Fatalf("data[%d] != 1. %+v", i, data)
		}
	}
	t.Logf("process ok")
}
