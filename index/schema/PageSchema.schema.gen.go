package schema

import (
	"io"
	"time"
	"unsafe"
)

var (
	_ = unsafe.Sizeof(0)
	_ = io.ReadFull
	_ = time.Now()
)

type PersistentLeafPage struct {
	PageType byte
	Pairs    []PersistentKeyValuePair
}

func (d *PersistentLeafPage) Size() (s uint64) {

	{
		l := uint64(len(d.Pairs))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}

		for k0 := range d.Pairs {

			{
				s += d.Pairs[k0].Size()
			}

		}

	}
	s += 1
	return
}
func (d *PersistentLeafPage) Marshal(buf []byte) ([]byte, error) {
	size := d.Size()
	{
		if uint64(cap(buf)) >= size {
			buf = buf[:size]
		} else {
			buf = make([]byte, size)
		}
	}
	i := uint64(0)

	{
		buf[0] = d.PageType
	}
	{
		l := uint64(len(d.Pairs))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+1] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+1] = byte(t)
			i++

		}
		for k0 := range d.Pairs {

			{
				nbuf, err := d.Pairs[k0].Marshal(buf[i+1:])
				if err != nil {
					return nil, err
				}
				i += uint64(len(nbuf))
			}

		}
	}
	return buf[:i+1], nil
}

func (d *PersistentLeafPage) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	{
		d.PageType = buf[i+0]
	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+1] & 0x7F)
			for buf[i+1]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+1]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		if uint64(cap(d.Pairs)) >= l {
			d.Pairs = d.Pairs[:l]
		} else {
			d.Pairs = make([]PersistentKeyValuePair, l)
		}
		for k0 := range d.Pairs {

			{
				ni, err := d.Pairs[k0].Unmarshal(buf[i+1:])
				if err != nil {
					return 0, err
				}
				i += ni
			}

		}
	}
	return i + 1, nil
}

type PersistentNonLeafPage struct {
	PageType     byte
	Pairs        []PersistentKeyValuePair
	ChildPageIds []uint32
}

func (d *PersistentNonLeafPage) Size() (s uint64) {

	{
		l := uint64(len(d.Pairs))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}

		for k0 := range d.Pairs {

			{
				s += d.Pairs[k0].Size()
			}

		}

	}
	{
		l := uint64(len(d.ChildPageIds))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}

		s += 4 * l

	}
	s += 1
	return
}
func (d *PersistentNonLeafPage) Marshal(buf []byte) ([]byte, error) {
	size := d.Size()
	{
		if uint64(cap(buf)) >= size {
			buf = buf[:size]
		} else {
			buf = make([]byte, size)
		}
	}
	i := uint64(0)

	{
		buf[0] = d.PageType
	}
	{
		l := uint64(len(d.Pairs))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+1] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+1] = byte(t)
			i++

		}
		for k0 := range d.Pairs {

			{
				nbuf, err := d.Pairs[k0].Marshal(buf[i+1:])
				if err != nil {
					return nil, err
				}
				i += uint64(len(nbuf))
			}

		}
	}
	{
		l := uint64(len(d.ChildPageIds))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+1] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+1] = byte(t)
			i++

		}
		for k0 := range d.ChildPageIds {

			{

				buf[i+0+1] = byte(d.ChildPageIds[k0] >> 0)

				buf[i+1+1] = byte(d.ChildPageIds[k0] >> 8)

				buf[i+2+1] = byte(d.ChildPageIds[k0] >> 16)

				buf[i+3+1] = byte(d.ChildPageIds[k0] >> 24)

			}

			i += 4

		}
	}
	return buf[:i+1], nil
}

func (d *PersistentNonLeafPage) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	{
		d.PageType = buf[i+0]
	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+1] & 0x7F)
			for buf[i+1]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+1]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		if uint64(cap(d.Pairs)) >= l {
			d.Pairs = d.Pairs[:l]
		} else {
			d.Pairs = make([]PersistentKeyValuePair, l)
		}
		for k0 := range d.Pairs {

			{
				ni, err := d.Pairs[k0].Unmarshal(buf[i+1:])
				if err != nil {
					return 0, err
				}
				i += ni
			}

		}
	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+1] & 0x7F)
			for buf[i+1]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+1]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		if uint64(cap(d.ChildPageIds)) >= l {
			d.ChildPageIds = d.ChildPageIds[:l]
		} else {
			d.ChildPageIds = make([]uint32, l)
		}
		for k0 := range d.ChildPageIds {

			{

				d.ChildPageIds[k0] = 0 | (uint32(buf[i+0+1]) << 0) | (uint32(buf[i+1+1]) << 8) | (uint32(buf[i+2+1]) << 16) | (uint32(buf[i+3+1]) << 24)

			}

			i += 4

		}
	}
	return i + 1, nil
}

type PersistentKeyValuePair struct {
	Key   []byte
	Value []byte
}

func (d *PersistentKeyValuePair) Size() (s uint64) {

	{
		l := uint64(len(d.Key))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	{
		l := uint64(len(d.Value))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	return
}
func (d *PersistentKeyValuePair) Marshal(buf []byte) ([]byte, error) {
	size := d.Size()
	{
		if uint64(cap(buf)) >= size {
			buf = buf[:size]
		} else {
			buf = make([]byte, size)
		}
	}
	i := uint64(0)

	{
		l := uint64(len(d.Key))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		copy(buf[i+0:], d.Key)
		i += l
	}
	{
		l := uint64(len(d.Value))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		copy(buf[i+0:], d.Value)
		i += l
	}
	return buf[:i+0], nil
}

func (d *PersistentKeyValuePair) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		if uint64(cap(d.Key)) >= l {
			d.Key = d.Key[:l]
		} else {
			d.Key = make([]byte, l)
		}
		copy(d.Key, buf[i+0:])
		i += l
	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		if uint64(cap(d.Value)) >= l {
			d.Value = d.Value[:l]
		} else {
			d.Value = make([]byte, l)
		}
		copy(d.Value, buf[i+0:])
		i += l
	}
	return i + 0, nil
}
