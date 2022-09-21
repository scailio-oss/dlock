/*
 *    Copyright 2022 scailio GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package logger

import (
	"context"
	"log"

	"github.com/scailio-oss/dlock/logger"
)

type defaultLogger struct {
}

func Default() logger.Logger {
	return &defaultLogger{}
}

func (d defaultLogger) Debug(_ context.Context, msg string, param ...any) {
	log.Printf("DEBUG %v %+q\n", msg, param)
}

func (d defaultLogger) Info(_ context.Context, msg string, param ...any) {
	log.Printf("INFO %v %+q\n", msg, param)
}

func (d defaultLogger) Warn(_ context.Context, msg string, param ...any) {
	log.Printf("WARN %v %+q\n", msg, param)
}

func (d defaultLogger) Error(_ context.Context, msg string, param ...any) {
	log.Printf("ERROR %v %+q\n", msg, param)
}
