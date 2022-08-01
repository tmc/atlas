// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package spanner

import (
	"github.com/DATA-DOG/go-sqlmock"
)

type mock struct {
	sqlmock.Sqlmock
}