// Copyright 2021 The Parca Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunkenc

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRLEChunk(t *testing.T) {
	c := NewRLEChunk()

	// check empty chunk
	require.Equal(t, []byte{0, 0, 0, 0}, c.Bytes())
	app, err := c.Appender()
	require.NoError(t, err)

	app.Append(0)
	require.Equal(t, []byte{0, 1, 0, 1, 0, 0, 1, 0}, c.Bytes())
	require.Equal(t, 1, c.NumSamples())
	app.Append(0)
	require.Equal(t, []byte{0, 2, 0, 1, 0, 0, 2, 0}, c.Bytes())
	require.Equal(t, 2, c.NumSamples())

	// Append 1 twice and then 5 more times
	app.Append(1)
	require.Equal(t, []byte{0, 3, 0, 2, 0, 0, 2, 2, 0, 1, 0}, c.Bytes())
	require.Equal(t, 3, c.NumSamples())

	app.Append(1)
	require.Equal(t, []byte{0, 4, 0, 2, 0, 0, 2, 2, 0, 2, 0}, c.Bytes())
	require.Equal(t, 4, c.NumSamples())

	for i := 0; i < 5; i++ {
		app.Append(1)
	}
	require.Equal(t, []byte{0, 9, 0, 2, 0, 0, 2, 2, 0, 7, 0}, c.Bytes())
	require.Equal(t, 9, c.NumSamples())

	// Append 2 twice to test another value
	app.Append(2)
	require.Equal(t, []byte{0, 10, 0, 3, 0, 0, 2, 2, 0, 7, 4, 0, 1, 0}, c.Bytes())
	require.Equal(t, 10, c.NumSamples())
	app.Append(2)
	require.Equal(t, []byte{0, 11, 0, 3, 0, 0, 2, 2, 0, 7, 4, 0, 2, 0}, c.Bytes())
	require.Equal(t, 11, c.NumSamples())

	// Append 3 100x to get a lot of the same values.
	for i := 0; i < 100; i++ {
		app.Append(3)
	}
	require.Equal(t, []byte{0, 111, 0, 4, 0, 0, 2, 2, 0, 7, 4, 0, 2, 6, 0, 100, 0}, c.Bytes())
	require.Equal(t, 111, c.NumSamples())

	// Iterate over the first values manually
	it := c.Iterator(nil)
	it.Next()
	require.Equal(t, int64(0), it.At())
	it.Next()
	require.Equal(t, int64(0), it.At())

	it.Next()
	require.Equal(t, int64(1), it.At())
	it.Next()
	require.Equal(t, int64(1), it.At())

	for i := 0; i < 5; i++ {
		it.Next()
		require.Equal(t, int64(1), it.At())
	}

	it.Next()
	require.Equal(t, int64(2), it.At())
	it.Next()
	require.Equal(t, int64(2), it.At())

	for it.Next() {
		require.NoError(t, it.Err())
		require.Equal(t, int64(3), it.At())
	}

	require.NoError(t, it.Err())
	require.False(t, it.Next())
}

func Test_RLEChunk_Insert(t *testing.T) {
	c := NewRLEChunk()
	app, err := c.Appender()
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		app.Append(1)
		require.Equal(t, i+1, c.NumSamples())
	}
	require.Equal(t, []byte{0, 3, 0, 1, 2, 0, 3, 0}, c.Bytes())

	// Insert in-place
	app.(*rleAppender).Insert(1, 1)
	require.Equal(t, []byte{0, 4, 0, 1, 2, 0, 4, 0}, c.Bytes())

	for i := 0; i < 2; i++ {
		app.Append(3)
	}
	require.Equal(t, []byte{0, 6, 0, 2, 2, 0, 4, 6, 0, 2, 0}, c.Bytes())

	fmt.Println(c.Bytes())
	it := c.Iterator(nil)
	for it.Next() {
		fmt.Printf("%v,", it.At())
	}
	fmt.Printf("\n")

	// Insert out of place; clean split
	app.(*rleAppender).Insert(4, 2)
	require.Equal(t, []byte{0, 7, 0, 3, 2, 0, 4, 4, 0, 1, 6, 0, 2, 0}, c.Bytes())

	fmt.Println(c.Bytes())
	it = c.Iterator(nil)
	for it.Next() {
		fmt.Printf("%v,", it.At())
	}
	fmt.Printf("\n")

	// Insert out of place; clean split
	app.(*rleAppender).Insert(5, 7)
	require.Equal(t, []byte{0, 8, 0, 4, 2, 0, 4, 4, 0, 1, 14, 0, 1, 6, 0, 2, 0}, c.Bytes())

	fmt.Println(c.Bytes())
	it = c.Iterator(nil)
	for it.Next() {
		fmt.Printf("%v,", it.At())
	}
	fmt.Printf("\n")

	// Insert in place;
	app.(*rleAppender).Insert(5, 7)
	require.Equal(t, []byte{0, 9, 0, 4, 2, 0, 4, 4, 0, 1, 14, 0, 2, 6, 0, 2, 0}, c.Bytes())

	fmt.Println(c.Bytes())
	it = c.Iterator(nil)
	for it.Next() {
		fmt.Printf("%v,", it.At())
	}
	fmt.Printf("\n")

	// Insert in place;
	app.(*rleAppender).Insert(0, 1)
	require.Equal(t, []byte{0, 10, 0, 4, 2, 0, 5, 4, 0, 1, 14, 0, 1, 6, 0, 2, 0}, c.Bytes())

	fmt.Println(c.Bytes())
	it = c.Iterator(nil)
	for it.Next() {
		fmt.Printf("%v,", it.At())
	}
	fmt.Printf("\n")
}
