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

package bolt_test

import (
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/janos/forky"
	"github.com/janos/forky/bolt"
	"github.com/janos/forky/test"
)

func TestBoltForkySync(t *testing.T) {
	testBoltForky(t, false)
}

func TestBoltForkyNoSync(t *testing.T) {
	testBoltForky(t, true)
}

func testBoltForky(t *testing.T, noSync bool) {
	t.Helper()

	test.StoreSuite(t, func(t *testing.T) (forky.Interface, func()) {
		path, err := ioutil.TempDir("", "swarm-forky-")
		if err != nil {
			t.Fatal(err)
		}

		metaStore, err := bolt.NewMetaStore(filepath.Join(path, "test.db"), noSync)
		if err != nil {
			t.Fatal(err)
		}

		return test.NewForkyStore(t, path, metaStore)
	})
}
