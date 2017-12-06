// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"strings"
)

func buildConditionSQL(columnName string, value int32) string {
	sql := fmt.Sprintf("`%s`", columnName)
	if value == ddlTestValueNull {
		sql += " IS NULL"
	} else {
		sql += fmt.Sprintf(" = %d", value)
	}
	return sql
}

func padLeft(str, pad string, length int) string {
	padding := strings.Repeat(pad, length)
	str = padding + str
	return str[len(str)-length:]
}

func padRight(str, pad string, length int) string {
	padding := strings.Repeat(pad, length)
	str = str + padding
	return str[:length]
}

// parallel run functions in parallel and wait until all of them are completed.
// If one of them returns error, the result is that error.
func parallel(funcs ...func() error) error {
	cr := make(chan error, len(funcs))
	for _, foo := range funcs {
		go func(foo func() error) {
			err := foo()
			cr <- err
		}(foo)
	}
	var err error
	for i := 0; i < len(funcs); i++ {
		r := <-cr
		if r != nil {
			err = r
		}
	}
	return err
}
