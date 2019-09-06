// Copyright 2019 The Swarm Authors
// This file is part of the Swarm library.
//
// The Swarm library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The Swarm library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the Swarm library. If not, see <http://www.gnu.org/licenses/>.

package badger_test

import (
	"io/ioutil"
	"testing"

	"github.com/janos/forky"
	"github.com/janos/forky/badger"
	"github.com/janos/forky/test"
)

func TestBadgerForky(t *testing.T) {
	test.StoreSuite(t, []int{1000, 10000, 100000, 1000000}, func(t *testing.T) (forky.Interface, func()) {
		path, err := ioutil.TempDir("", "swarm-forky-")
		if err != nil {
			t.Fatal(err)
		}

		metaStore, err := badger.NewMetaStore(path)
		if err != nil {
			t.Fatal(err)
		}

		return test.NewForkyStore(t, path, metaStore)
	})
}
