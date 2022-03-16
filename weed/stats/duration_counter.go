package stats

import (
	"time"
)

type TimedValue struct {
	t   time.Time
	val int64
}

func NewTimedValue(t time.Time, val int64) *TimedValue {
	return &TimedValue{t: t, val: val}
}

type RoundRobinCounter struct {
	LastIndex int
	Values    []int64
	Counts    []int64
}

func NewRoundRobinCounter(slots int) *RoundRobinCounter {
	return &RoundRobinCounter{LastIndex: -1, Values: make([]int64, slots), Counts: make([]int64, slots)}
}
func (rrc *RoundRobinCounter) Add(index int, val int64) {
	if index >= len(rrc.Values) {
		return
	}
	for rrc.LastIndex != index {
		rrc.LastIndex = (rrc.LastIndex + 1) % len(rrc.Values)
		// 这里为什么清零了？
		// 为了获得 当前一秒、一分、一天等 的数据
		// 此时 LastIndex和index 之间的数据应当是上一个循环（比如一分钟前该秒）中的数据，旧数据应当清零
		rrc.Values[rrc.LastIndex] = 0
		rrc.Counts[rrc.LastIndex] = 0
	}
	rrc.Values[index] += val
	rrc.Counts[index]++
}

//返回一个单位时间的最大成功操作数
func (rrc *RoundRobinCounter) Max() (max int64) {
	for _, val := range rrc.Values {
		if max < val {
			max = val
		}
	}
	return
}

//返回所有单位时间的操作数之和
func (rrc *RoundRobinCounter) Count() (cnt int64) {
	for _, c := range rrc.Counts {
		cnt += c
	}
	return
}

//返回所有单位时间的成功操作数之和
func (rrc *RoundRobinCounter) Sum() (sum int64) {
	for _, val := range rrc.Values {
		sum += val
	}
	return
}

//将所有的单位时间内的数值append成一个切片返回
func (rrc *RoundRobinCounter) ToList() (ret []int64) {
	index := rrc.LastIndex
	step := len(rrc.Values)
	for step > 0 {
		step--
		index++
		if index >= len(rrc.Values) {
			index = 0
		}
		ret = append(ret, rrc.Values[index])
	}
	return
}

type DurationCounter struct {
	MinuteCounter *RoundRobinCounter
	HourCounter   *RoundRobinCounter
	DayCounter    *RoundRobinCounter
	WeekCounter   *RoundRobinCounter
}

func NewDurationCounter() *DurationCounter {
	return &DurationCounter{
		MinuteCounter: NewRoundRobinCounter(60),
		HourCounter:   NewRoundRobinCounter(60),
		DayCounter:    NewRoundRobinCounter(24),
		WeekCounter:   NewRoundRobinCounter(7),
	}
}

// Add is for cumulative counts
// 为每个单位时间计数
func (sc *DurationCounter) Add(tv *TimedValue) {
	sc.MinuteCounter.Add(tv.t.Second(), tv.val)
	sc.HourCounter.Add(tv.t.Minute(), tv.val)
	sc.DayCounter.Add(tv.t.Hour(), tv.val)
	sc.WeekCounter.Add(int(tv.t.Weekday()), tv.val)
}
