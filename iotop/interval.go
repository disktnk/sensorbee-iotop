package iotop

import (
	"fmt"
	"strconv"
	"time"
)

func updateInterval(ms *monitoringState, eb *editBox) (done chan struct{}) {
	done = make(chan struct{}, 1)
	defer close(done)
	defer eb.reset()

	in, err := eb.start(fmt.Sprintf("Current %v, change to [sec]: ", ms.d))
	if err != nil {
		eb.redrawAll(fmt.Sprintf("fail to get key events, %v", err))
		return
	}
	if in == "" {
		return
	}
	interval, err := strconv.ParseFloat(in, 64)
	if err != nil {
		eb.redrawAll(fmt.Sprintf("fail to parse input string, %v", err))
		return
	}
	ms.d = time.Duration(interval*1000) * time.Millisecond
	return
}
