// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package game

import (
	"context"
	"errors"
	"log"
	"strconv"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

type GameEvent struct {
	User      string
	Team      string
	Score     int64
	Timestamp int64
}

func (g GameEvent) GetKey(field string) string {
	if field == "team" {
		return g.Team
	}
	return g.User
}

type ParseEventFn struct {
	Sep       string `json:"sep"`
	NumFields int    `json:"num_fields"`

	NumParseErrors *beam.Counter
	err            error
}

// Parses the raw game event info into GameEvent objects. Each event line
// has the following format:
// username,teamname,score,timestamp_in_ms,readable_time
// user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224
// The human-readable time string is not used here.
func (pe *ParseEventFn) ProcessElement(ctx context.Context, line string, emit func(GameEvent)) {
	tokens := strings.Split(line, pe.Sep)
	if len(tokens) != pe.NumFields {
		pe.handleErr(ctx, errors.New("invalid format line"), line)
		return
	}
	user := strings.TrimSpace(tokens[0])
	team := strings.TrimSpace(tokens[1])
	score, err := strconv.ParseInt(strings.TrimSpace(tokens[2]), 10, 64)
	pe.handleErr(ctx, err, line)
	timestamp, err := strconv.ParseInt(strings.TrimSpace(tokens[3]), 10, 64)
	pe.handleErr(ctx, err, line)

	if pe.err != nil {
		return
	}

	emit(GameEvent{User: user, Team: team, Score: score, Timestamp: timestamp})
}

func (pe *ParseEventFn) handleErr(ctx context.Context, err error, line string) {
	if err != nil {
		pe.err = err
		pe.NumParseErrors.Inc(ctx, 1)
		// errors.Wrap()
		log.Printf("Parse error: err=%s, line=%s", err.Error(), line)
		return
	}
}

type ExtractAndSumScore struct {
	Field string `json:"field"`
}

func (es *ExtractAndSumScore) mapKVScoreByField(game GameEvent, emit func(key string, value int64)) {
	emit(game.GetKey(es.Field), game.Score)
}

func (es *ExtractAndSumScore) Expand(scope beam.Scope, games beam.PCollection) beam.PCollection {
	kv := beam.ParDo(scope, es.mapKVScoreByField, games)
	return stats.SumPerKey(scope, kv)
}
