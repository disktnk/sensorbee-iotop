package iotop

import (
	"bytes"
	"fmt"
	"sync"
	"text/tabwriter"
	"time"

	"gopkg.in/sensorbee/sensorbee.v0/data"
)

type lineHolder struct {
	rwm     sync.RWMutex
	srcs    []sourceLine
	boxes   []boxLine
	sinks   []sinkLine
	edges   map[string]*edgeLine
	current time.Time
	decoder *data.Decoder
}

func newLineHolder() *lineHolder {
	return &lineHolder{
		srcs:    []sourceLine{},
		boxes:   []boxLine{},
		sinks:   []sinkLine{},
		edges:   map[string]*edgeLine{},
		current: time.Now(),
		decoder: data.NewDecoder(nil),
	}
}

func (h *lineHolder) clear() {
	h.srcs = []sourceLine{}
	h.boxes = []boxLine{}
	h.sinks = []sinkLine{}
	h.edges = map[string]*edgeLine{}
}

func (h *lineHolder) push(m data.Map) error {
	h.rwm.Lock()
	defer h.rwm.Unlock()
	ns := &nodeStatus{}
	if err := h.decoder.Decode(m, ns); err != nil {
		return err
	}

	if h.current != ns.Timestamp {
		h.clear()
		h.current = ns.Timestamp
	}

	gl := &generalLine{
		name:     ns.NodeName,
		nodeType: ns.NodeType,
		state:    ns.State,
	}
	switch ns.NodeType {
	case "source":
		line := sourceLine{
			generalLine: gl,
			out:         ns.OutputStats.NumSentTotal,
			dropped:     ns.OutputStats.NumDropped,
		}
		h.srcs = append(h.srcs, line)
		h.setSourcePipeStatus(ns.NodeName, ns.NodeType, ns.OutputStats.Outputs)

	case "box":
		line := boxLine{
			generalLine: gl,
			inOut: ns.OutputStats.NumSentTotal -
				ns.InputStats.NumReceivedTotal,
			dropped: ns.OutputStats.NumDropped,
			nerror:  ns.InputStats.NumErrors,
		}
		// TODO: process time
		// TODO: BQL statement, when SELETE query
		h.boxes = append(h.boxes, line)
		h.setSourcePipeStatus(ns.NodeName, ns.NodeType, ns.OutputStats.Outputs)
		h.setDestinationPipeStatus(ns.NodeName, ns.NodeType, ns.InputStats.Inputs)

	case "sink":
		line := sinkLine{
			generalLine: gl,
			in:          ns.InputStats.NumReceivedTotal,
			nerror:      ns.InputStats.NumErrors,
		}
		h.sinks = append(h.sinks, line)
		h.setDestinationPipeStatus(ns.NodeName, ns.NodeType, ns.InputStats.Inputs)
	}
	return nil
}

func (h *lineHolder) flush() string {
	h.rwm.RLock()
	defer h.rwm.RUnlock()
	b := bytes.NewBuffer(nil)
	w := tabwriter.NewWriter(b, 0, 0, 1, ' ', 0)
	fmt.Fprintln(w, "SENDER\tSTYPE\tRCVER\tRTYPE\tSQSIZE\tSQNUM\tSNUM\tRQSIZE\tRQNUM\tRNUM\tINOUT")
	for _, l := range h.edges {
		values := fmt.Sprintf("%v\t%v\t%v\t%v\t%d\t%d\t%d\t%d\t%d\t%d\t%d",
			l.senderName, l.senderNodeType, l.receiverName,
			l.receiverNodeType, l.senderQueueSize, l.senderQueued, l.sent,
			l.receiverQueueSize, l.receiverQueued, l.received, l.inOut)
		fmt.Fprintln(w, values)
	}
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "NAME\tNTYPE\tSTATE\tOUT\tDROP")
	for _, l := range h.srcs {
		values := fmt.Sprintf("%v\t%v\t%v\t%d\t%d",
			l.name, l.nodeType, l.state, l.out, l.dropped)
		fmt.Fprintln(w, values)
	}
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "NAME\tNTYPE\tSTATE\tINOUT\tDROP\tERR")
	for _, l := range h.boxes {
		values := fmt.Sprintf("%v\t%v\t%v\t%d\t%d\t%d",
			l.name, l.nodeType, l.state, l.inOut, l.dropped, l.nerror)
		fmt.Fprintln(w, values)
	}
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "NAME\tNTYPE\tSTATE\tIN\tERR")
	for _, l := range h.sinks {
		values := fmt.Sprintf("%v\t%v\t%v\t%d\t%d",
			l.name, l.nodeType, l.state, l.in, l.nerror)
		fmt.Fprintln(w, values)
	}

	w.Flush()
	return b.String()
}

func (h *lineHolder) setSourcePipeStatus(name, nodeType string, outputs data.Map) {
	if len(outputs) == 0 {
		return
	}
	for outName, output := range outputs {
		om, _ := data.AsMap(output)
		pipeSts := &sourcePipeStatus{}
		if err := h.decoder.Decode(om, pipeSts); err != nil {
			return
		}

		key := fmt.Sprintf("%s|%s", name, outName)
		line, ok := h.edges[key]
		if !ok {
			line = &edgeLine{}
			h.edges[key] = line
		} else {
			line.inOut = line.received - pipeSts.NumSent
		}
		line.senderName = name
		line.senderNodeType = nodeType

		line.senderQueued = pipeSts.NumQueued
		line.senderQueueSize = pipeSts.QueueSize
		line.sent = pipeSts.NumSent
	}
}

func (h *lineHolder) setDestinationPipeStatus(name, nodeType string, inputs data.Map) {
	if len(inputs) == 0 {
		return
	}
	for inName, input := range inputs {
		im, _ := data.AsMap(input)
		pipeSts := &destinationPipeStatus{}
		if err := h.decoder.Decode(im, pipeSts); err != nil {
			return
		}

		key := fmt.Sprintf("%s|%s", inName, name)
		line, ok := h.edges[key]
		if !ok {
			line = &edgeLine{}
			h.edges[key] = line
		} else {
			line.inOut = pipeSts.NumReceived - line.sent
		}
		line.receiverName = name
		line.receiverNodeType = nodeType

		line.receiverQueued = pipeSts.NumQueued
		line.receiverQueueSize = pipeSts.QueueSize
		line.received = pipeSts.NumReceived
	}
}
