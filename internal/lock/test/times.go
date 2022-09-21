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

package test

import (
	"time"
)

var Step = 1 * time.Second
var T1 = time.Unix(1, 0)
var T2 = T1.Add(Step)
var T3 = T2.Add(Step)
var T4 = T3.Add(Step)
var T5 = T4.Add(Step)

const TimeoutDuration = 1 * time.Second
