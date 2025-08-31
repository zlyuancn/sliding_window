package sliding_window

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestSlidingWindowAndWait(t *testing.T) {
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

// 不同的滑动窗口大小跑数据速度
func BenchmarkWindowSizeAnd1e8(b *testing.B) {
	windowSize := []struct {
		name       string
		windowSize int
	}{
		{"windowSize_10", 10},
		{"windowSize_100", 100},
		{"windowSize_1000", 1000},
		{"windowSize_10000", 10000},
	}

	for _, win := range windowSize {
		b.Run(win.name, func(b *testing.B) {
			sw := NewSlidingWindow(win.windowSize, 1)
			defer sw.Stop()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					sn, err := sw.Next(context.Background())
					if err != nil {
						b.Fatalf("next err: %v", err)
					}
					go sw.Ack(sn)
				}
			})
		})
	}
}

func BenchmarkMultithreading(b *testing.B) {
	threadSize := []int{10, 100, 1000, 10000}                   // 线程数量
	windowSizePower := []float32{1, 1.3, 1.5, 1.8, 2, 3, 5, 10} // 窗口数量是线程数量的多少倍

	for _, ts := range threadSize {
		for _, wsp := range windowSizePower {
			name := fmt.Sprintf("%d_%.1f", ts, wsp)
			b.Run(name, func(b *testing.B) {
				wSize := int(float32(ts) * wsp)
				sw := NewSlidingWindow(wSize, 1)
				defer sw.Stop()

				lock := make(chan struct{}, ts)

				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						lock <- struct{}{} // 占用一个协程
						sn, err := sw.Next(context.Background())
						if err != nil {
							b.Fatalf("next err: %v", err)
						}
						go func() {
							time.Sleep(time.Millisecond * time.Duration(rand.Int31n(50))) // 模拟延迟
							sw.Ack(sn)
							<-lock // 释放一个协程
						}()
					}
				})
			})
		}

	}
}
