package bus_test

import (
	"testing"

	"github.com/gopatchy/bus"
	"github.com/gopatchy/metadata"
	"github.com/stretchr/testify/require"
)

func TestBus(t *testing.T) {
	t.Parallel()

	bus := bus.NewBus()

	// Announce with no subscribers
	bus.Announce("busTest1", &busTest{
		Metadata: metadata.Metadata{
			ID: "id-nosub",
		},
	})

	// Complex subscription layout
	c1a := bus.SubscribeKey("busTest1", "id-overlap", nil)
	require.Nil(t, <-c1a)

	defer bus.UnsubscribeKey("busTest1", "id-overlap", c1a)

	c2a := bus.SubscribeKey("busTest2", "id-overlap", nil)
	require.Nil(t, <-c2a)

	defer bus.UnsubscribeKey("busTest2", "id-overlap", c2a)

	c2b := bus.SubscribeKey("busTest2", "id-dupe", nil)
	require.Nil(t, <-c2b)

	defer bus.UnsubscribeKey("busTest2", "id-dupe", c2b)

	c2c := bus.SubscribeKey("busTest2", "id-dupe", nil)
	require.Nil(t, <-c2c)

	defer bus.UnsubscribeKey("busTest2", "id-dupe", c2c)

	ct1 := bus.SubscribeType("busTest1", nil)
	require.Nil(t, <-ct1)

	defer bus.UnsubscribeType("busTest1", ct1)

	ct2 := bus.SubscribeType("busTest2", nil)
	require.Nil(t, <-ct2)

	defer bus.UnsubscribeType("busTest2", ct2)

	// Overlapping IDs but not types
	bus.Announce("busTest1", &busTest{
		Metadata: metadata.Metadata{
			ID: "id-overlap",
		},
	})

	msg := <-c1a
	require.Equal(t, "id-overlap", msg.(*busTest).ID)

	msg = <-ct1
	require.Equal(t, "id-overlap", msg.(*busTest).ID)

	select {
	case msg := <-c2a:
		require.Fail(t, "unexpected message", msg)
	case msg := <-ct2:
		require.Fail(t, "unexpected message", msg)
	default:
	}

	bus.Announce("busTest2", &busTest{
		Metadata: metadata.Metadata{
			ID: "id-overlap",
		},
	})

	select {
	case msg := <-c1a:
		require.Fail(t, "unexpected message", msg)
	case msg := <-ct1:
		require.Fail(t, "unexpected message", msg)
	default:
	}

	msg = <-c2a
	require.Equal(t, "id-overlap", msg.(*busTest).ID)

	msg = <-ct2
	require.Equal(t, "id-overlap", msg.(*busTest).ID)

	bus.Announce("busTest2", &busTest{
		Metadata: metadata.Metadata{
			ID: "id-dupe",
		},
	})

	msg = <-c2b
	require.Equal(t, "id-dupe", msg.(*busTest).ID)

	msg = <-c2c
	require.Equal(t, "id-dupe", msg.(*busTest).ID)

	msg = <-ct2
	require.Equal(t, "id-dupe", msg.(*busTest).ID)
}

func TestBusDelete(t *testing.T) {
	t.Parallel()

	bus := bus.NewBus()

	c := bus.SubscribeKey("busTest", "id1", nil)
	require.Nil(t, <-c)

	defer bus.UnsubscribeKey("busTest", "id1", c)

	ct := bus.SubscribeType("busTest", nil)
	require.Nil(t, <-ct)

	defer bus.UnsubscribeType("busTest", ct)

	bus.Announce("busTest", &busTest{
		Metadata: metadata.Metadata{
			ID: "id1",
		},
	})

	msg := <-c
	require.Equal(t, "id1", msg.(*busTest).ID)

	msg = <-ct
	require.Equal(t, "id1", msg.(*busTest).ID)

	bus.Delete("busTest", "id1")

	_, ok := <-c
	require.False(t, ok)

	id := (<-ct).(string)
	require.Equal(t, "id1", id)
}

type busTest struct {
	metadata.Metadata
}
