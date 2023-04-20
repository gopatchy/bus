package bus

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/gopatchy/metadata"
)

type Bus struct {
	mu        sync.Mutex
	keyViews  map[string]map[uintptr]chan<- any
	typeViews map[string]map[uintptr]chan<- any
}

func NewBus() *Bus {
	return &Bus{
		keyViews:  map[string]map[uintptr]chan<- any{},
		typeViews: map[string]map[uintptr]chan<- any{},
	}
}

func (b *Bus) Announce(t string, obj any) {
	key := getObjKey(t, obj)

	b.mu.Lock()
	defer b.mu.Unlock()

	announce(obj, b.keyViews[key])
	announce(obj, b.typeViews[t])
}

func (b *Bus) Delete(t string, id string) {
	key := getKey(t, id)

	b.mu.Lock()
	defer b.mu.Unlock()

	for _, c := range b.keyViews[key] {
		close(c)
	}

	delete(b.keyViews, key)

	announce(id, b.typeViews[t])
}

func (b *Bus) SubscribeKey(t, id string, initial any) <-chan any {
	key := getKey(t, id)

	b.mu.Lock()
	defer b.mu.Unlock()

	ret := make(chan any, 100)

	ret <- initial

	if _, has := b.keyViews[key]; !has {
		b.keyViews[key] = map[uintptr]chan<- any{}
	}

	b.keyViews[key][chanID(ret)] = ret

	return ret
}

func (b *Bus) SubscribeType(t string, initial any) <-chan any {
	b.mu.Lock()
	defer b.mu.Unlock()

	ret := make(chan any, 100)

	ret <- initial

	if _, has := b.typeViews[t]; !has {
		b.typeViews[t] = map[uintptr]chan<- any{}
	}

	b.typeViews[t][chanID(ret)] = ret

	return ret
}

func (b *Bus) UnsubscribeKey(t, id string, c <-chan any) {
	key := getKey(t, id)

	b.mu.Lock()
	defer b.mu.Unlock()

	if cw, has := b.keyViews[key][chanID(c)]; has {
		close(cw)
		delete(b.keyViews[key], chanID(c))
	}

	if len(b.keyViews[key]) == 0 {
		delete(b.keyViews, key)
	}
}

func (b *Bus) UnsubscribeType(t string, c <-chan any) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if cw, has := b.typeViews[t][chanID(c)]; has {
		close(cw)
		delete(b.typeViews[t], chanID(c))
	}

	if len(b.typeViews[t]) == 0 {
		delete(b.typeViews, t)
	}
}

func getObjKey(t string, obj any) string {
	return getKey(t, metadata.GetMetadata(obj).ID)
}

func getKey(t string, id string) string {
	return fmt.Sprintf("%s:%s", t, id)
}

func announce(obj any, chans map[uintptr]chan<- any) {
	for id, c := range chans {
		select {
		case c <- obj:
		default:
			close(c)
			delete(chans, id)
		}
	}
}

func chanID(c <-chan any) uintptr {
	return reflect.ValueOf(c).Pointer()
}
