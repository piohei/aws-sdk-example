package tar

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"io"
	"sync"
)

type ringBuffer struct {
	sync.Mutex

	ranges      *priorityRanges
	isEof       bool
	bufPos      int
	bufStartPos int64
	buffer      []byte
	observers   []chan bool
}

func newRingBuffer(size int) *ringBuffer {
	return &ringBuffer{
		ranges:      newPriorityRanges(),
		isEof:       false,
		bufPos:      0,
		bufStartPos: 0,
		buffer:      make([]byte, size),
		observers:   make([]chan bool, 0),
	}
}

func (b *ringBuffer) WriteAt(p []byte, pos int64) (n int, err error) {
	log.Tracef("Writing len(p)=%v, pos=%v)", len(p), pos)
	if err = b.awaitInRange(pos, len(p)); err != nil {
		return 0, err
	}

	b.Lock()

	log.Tracef("Actual write len(p)=%v, pos=%v)", len(p), pos)
	bufPosOffset := (b.bufPos + int(pos-b.bufStartPos)) % len(b.buffer)
	if bufPosOffset+len(p) > len(b.buffer) {
		b1s := bufPosOffset
		b1e := len(b.buffer)
		p1s := 0
		p1e := b1e - b1s

		b2s := 0
		b2e := len(p) - p1e
		p2s := p1e
		p2e := len(p)

		log.Tracef("Copying buffer[%v:%v] from p[%v:%v]", b1s, b1e, p1s, p1e)
		copy(b.buffer[b1s:b1e], p[p1s:p1e])
		log.Tracef("Copying buffer[%v:%v] from p[%v:%v]", b2s, b2e, p2s, p2e)
		copy(b.buffer[b2s:b2e], p[p2s:p2e])
	} else {
		b1s := bufPosOffset
		b1e := b1s + len(p)
		p1s := 0
		p1e := len(p)

		log.Tracef("Copying buffer[%v:%v] from p[%v:%v]", b1s, b1e, p1s, p1e)
		copy(b.buffer[b1s:b1e], p[p1s:p1e])
	}

	log.Tracef("Pushing range [%v,%v)", pos, pos+int64(len(p)))
	b.ranges.Push(pos, pos+int64(len(p)))

	b.Unlock()

	b.notifyObservers()

	log.Tracef("Done writing len(p)=%v, pos=%v)", len(p), pos)

	return len(p), nil
}

func (b *ringBuffer) Read(p []byte) (n int, err error) {
	log.Tracef("Reading len(p)=%v starting bufStartPos=%v bufPos=%v)", len(p), b.bufStartPos, b.bufPos)
	for {
		b.Lock()
		rangeStart, rangeEnd := b.ranges.Peek()
		b.Unlock()

		if rangeStart < 0 && b.isEof {
			log.Tracef("Done reading (EOF) len(p)=%v)", len(p))
			return 0, io.EOF
		}

		if rangeStart < 0 || b.bufStartPos < rangeStart {
			c := b.registerObserver()
			<-c
			continue
		}

		b.Lock()
		rangeLen := min(int(rangeEnd-rangeStart), len(p))
		if b.bufPos+rangeLen > len(b.buffer) {
			b1s := b.bufPos
			b1e := len(b.buffer)
			p1s := 0
			p1e := b1e - b1s

			b2s := 0
			b2e := rangeLen - p1e
			p2s := p1e
			p2e := rangeLen

			log.Tracef("Copying p[%v:%v] from buffer[%v:%v]", p1s, p1e, b1s, b1e)
			copy(p[p1s:p1e], b.buffer[b1s:b1e])
			log.Tracef("Copying p[%v:%v] from buffer[%v:%v]", p2s, p2e, b2s, b2e)
			copy(p[p2s:p2e], b.buffer[b2s:b2e])
		} else {
			b1s := b.bufPos
			b1e := b1s + rangeLen
			p1s := 0
			p1e := rangeLen

			log.Tracef("Copying p[%v:%v] from buffer[%v:%v]", p1s, p1e, b1s, b1e)
			copy(p[p1s:p1e], b.buffer[b1s:b1e])
		}

		b.ranges.Pop()
		if rangeStart+int64(rangeLen) < rangeEnd {
			b.ranges.Push(rangeStart+int64(rangeLen), rangeEnd)
		}
		b.bufPos = (b.bufPos + rangeLen) % len(b.buffer)
		b.bufStartPos = rangeStart + int64(rangeLen)
		b.Unlock()

		b.notifyObservers()

		log.Tracef("Done reading len(p)=%v, rangeLen=%v, p[0:10]=%v)", len(p), rangeLen, p[0:min(10, len(p))])

		return rangeLen, nil
	}
}

func (b *ringBuffer) Close() {
	b.isEof = true
}

func (b *ringBuffer) awaitInRange(start int64, size int) error {
	if size > len(b.buffer) {
		return errors.New("buffer to small to fit data")
	}

	end := start + int64(size)
	for {
		b.Lock()
		rangeStart, rangeEnd := b.bufStartPos, b.bufStartPos+int64(len(b.buffer))
		b.Unlock()
		if rangeStart <= start && end < rangeEnd {
			return nil
		}

		c := b.registerObserver()
		log.Tracef("Awaiting for range [%v,%v)", start, start+int64(size))
		<-c
	}
}

func (b *ringBuffer) registerObserver() chan bool {
	b.Lock()
	defer b.Unlock()

	c := make(chan bool, 1)
	b.observers = append(b.observers, c)
	return c
}

func (b *ringBuffer) notifyObservers() {
	b.Lock()
	defer b.Unlock()
	for _, o := range b.observers {
		o <- true
	}
	b.observers = nil
}

type bufferRange struct {
	start, end int64
}

type priorityRanges struct {
	ranges []bufferRange
}

func newPriorityRanges() *priorityRanges {
	return &priorityRanges{
		ranges: make([]bufferRange, 0),
	}
}

func (r *priorityRanges) Push(start, end int64) {
	r.ranges = append(r.ranges, bufferRange{
		start: start,
		end:   end,
	})
	for i := len(r.ranges) - 2; i >= 0; i-- {
		if r.ranges[i].start >= r.ranges[i+1].start {
			return
		}

		r.ranges[i], r.ranges[i+1] = r.ranges[i+1], r.ranges[i]
	}
}

func (r *priorityRanges) Peek() (start, end int64) {
	if len(r.ranges) == 0 {
		return -1, -1
	}
	item := r.ranges[len(r.ranges)-1]
	return item.start, item.end
}

func (r *priorityRanges) Pop() (start, end int64) {
	if len(r.ranges) == 0 {
		return -1, -1
	}
	item := r.ranges[len(r.ranges)-1]
	r.ranges = r.ranges[:len(r.ranges)-1]
	return item.start, item.end
}
