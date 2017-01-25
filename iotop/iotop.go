package iotop

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/sensorbee/sensorbee.v0/data"

	"github.com/mattn/go-runewidth"
	"github.com/nsf/termbox-go"
	"gopkg.in/urfave/cli.v1"
)

// Run 'sensorbee-iotop' command.
func Run(c *cli.Context) error {
	// TODO: check os.Stdout().Fd() is terminal or not
	// ref: github.com/mattn/go-isatty

	req, err := newNodeStatusRequester(c.String("uri"), c.String("api-version"),
		c.String("topology"))
	if err != nil {
		return err
	}

	d := c.Float64("d")
	if d < 1.0 {
		return fmt.Errorf("interval must be over than 1[sec]")
	}
	return Monitor(d, req)
}

// Monitor I/O of each nodes.
func Monitor(d float64, req StatusRequester) error {
	if err := setupStatusQuery(req, 1.0); err != nil {
		return err
	}
	defer tearDownStatusQuery(req) //TODO: skip error
	res, err := selectNodeStatus(req)
	if err != nil {
		return err
	}
	defer res.Close()

	lh := newLineHolder()
	errChan := make(chan error, 1)
	ch, err := res.ReadStreamJSON()
	if err != nil {
		return err
	}
	go func() {
		for {
			v, err := data.NewValue(<-ch)
			if err != nil {
				errChan <- err
				return
			}
			m, err := data.AsMap(v)
			if err != nil {
				errChan <- err
				return
			}
			if err := lh.push(m); err != nil {
				errChan <- err
				return
			}
		}
	}()

	// setup termbox when all preparations are done, because initializing
	// termbox sometimes destroys terminal UI.
	if err := termbox.Init(); err != nil {
		return fmt.Errorf("fail to initialize termbox, %v", err)
	}
	defer termbox.Close()
	eventQueue := make(chan termbox.Event)
	go func() {
		for {
			eventQueue <- termbox.PollEvent()
		}
	}()

	tick := time.Tick(time.Duration(d*1000) * time.Millisecond)
	go func() {
		for {
			draw(lh.flush())
			<-tick
		}
	}()

	running := true
	for running {
		select {
		case ev := <-eventQueue:
			if ev.Type == termbox.EventKey &&
				(ev.Key == termbox.KeyCtrlC || ev.Ch == 'q') {
				running = false
			}
		case err := <-errChan:
			return err
		default:
			// nothing to do
		}
	}
	return nil
}

const iotopTerminalColor = termbox.ColorDefault

func draw(lines string) {
	termbox.Clear(iotopTerminalColor, iotopTerminalColor)
	for i, line := range strings.Split(lines, "\n") {
		tbprint(0, i, iotopTerminalColor, iotopTerminalColor, line)
	}
	termbox.Flush()
}

func tbprint(x, y int, fg, bg termbox.Attribute, msg string) {
	for _, c := range msg {
		termbox.SetCell(x, y, c, fg, bg)
		x += runewidth.RuneWidth(c)
	}
}
