package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
)

//TODO: Add license
//TODO: Add doc
var (
	input = flag.String("input", "gs://dataflow-samples/game/5000_gaming_data.csv", "File(s) to read.")

	// Set this required option to specify where to write the output.
	output = flag.String("output", "", "Output file (required).")
)

var (
	numParseErrors = beam.NewCounter("main", "ParseErrors")
)

// TODO: fold into common
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

func main() {
	flag.Parse()

	beam.Init()

	// Input validation is done as usual. Note that it must be after Init().
	if *output == "" {
		log.Fatal("No output provided")
	}

	pipeline := beam.NewPipeline()
	scope := pipeline.Root()
	// TODO: fallback to local text list
	// lines := textio.Read(s, *input)
	lines := beam.CreateList(scope.Scope("ReadInputTextFile"), []string{
		"user9_BattleshipGreyPossum,BattleshipGreyPossum,14,1447719060000,2015-11-16 16:11:03.955",
		"user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224",
		"user0_AzureBilby,AzureBilby,8,1447719060000,2015-11-16 16:11:03.955",
		"user9_AzureBilby,AzureBilby,5,1447719060000,2015-11-16 16:11:03.959",
		"user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224"})

	// Parse csv input into Game Event
	parsedEvents := beam.ParDo(scope.Scope("ParseEventFn"), &ParseEventFn{Sep: ",", NumFields: 5}, lines)
	// Extract and sum username/score pairs from the event data.
	userTotalScore := (&ExtractAndSumScore{Field: "user"}).Expand(scope, parsedEvents)
	// Format the result
	formattedResults := beam.ParDo(scope, &FormatResult{}, userTotalScore)
	// Finally write the formatted results to text file
	textio.Write(scope, *output, formattedResults)

	if err := beamx.Run(context.Background(), pipeline); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}

type ParseEventFn struct {
	Sep       string
	NumFields int

	err error
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
		numParseErrors.Inc(ctx, 1)
		// errors.Wrap()
		log.Printf("Parse error: err=%s, line=%s", err.Error(), line)
		return
	}
}

type ExtractAndSumScore struct {
	Field string
}

func (es *ExtractAndSumScore) mapKVScoreByField(game GameEvent, emit func(key string, value int64)) {
	emit(game.GetKey(es.Field), game.Score)
}

func (es *ExtractAndSumScore) Expand(scope beam.Scope, games beam.PCollection) beam.PCollection {
	scope = scope.Scope("ExtractAndSumScore")
	kv := beam.ParDo(scope, es.mapKVScoreByField, games)
	return stats.SumPerKey(scope, kv)
}

type FormatResult struct {
}

// TODO: generalize this
// formatFn is a DoFn that formats a word and its count as a string.
func (fr *FormatResult) formatFn(ctx context.Context, userOrTeam string, totalScore int64) string {
	return fmt.Sprintf("user: %s, total_score: %v", userOrTeam, totalScore)
}

func (fr *FormatResult) ProcessElement(ctx context.Context, userOrTeam string, totalScore int64, emit func(string)) {
	emit(fr.formatFn(ctx, userOrTeam, totalScore))
}
