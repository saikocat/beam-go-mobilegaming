package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/apache/beam/sdks/go/examples/complete/game"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	//	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/filter"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/go/pkg/beam/x/debug"
)

var (
	startMinTimestamp = int64(1)
	stopMinTimestamp  = int64(1)
	now               = time.Now()
)

var (
	numParseErrors = beam.NewCounter("main", "ParseErrors")
)

func main() {
	flag.Parse()

	beam.Init()

	pipeline := beam.NewPipeline()
	scope := pipeline.Root()

	// TODO: fallback to local text list
	// lines := textio.Read(s, *input)
	lines := beam.CreateList(scope.Scope("ReadInputTextFile"), []string{
		"user9_BattleshipGreyPossum,BattleshipGreyPossum,14,1447719060000,2015-11-16 16:11:03.955",
		"user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224",
		"user2_AzureBilby,AsparagusPig,8,1447719060000,2015-11-16 16:11:03.955",
		"user0_AzureBilby,AzureBilby,8,1447719060000,2015-11-16 16:11:03.955",
		"user9_AzureBilby,AzureBilby,5,1447719060000,2015-11-16 16:11:03.959",
		"user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224"})

	// Parse csv input into Game Event
	parseEventFn := &game.ParseEventFn{Sep: ",", NumFields: 5, NumParseErrors: &numParseErrors}
	parsedEvents := beam.ParDo(scope.Scope("ParseEventFn"), parseEventFn, lines)
	debug.Printf(scope, "%+v", parsedEvents)

	// Filter out data before and after the given times so that it is not included
	// in the calculations. As we collect data in batches (say, by day), the batch for the day
	// that we want to analyze could potentially include some late-arriving data from the
	// previous day.
	// If so, we want to weed it out. Similarly, if we include data from the following day
	// (to scoop up late-arriving events from the day we're analyzing), we need to weed out
	// events that fall after the time period we want to analyze.
	filteredStartTime := filter.Include(scope, parsedEvents, func(g game.GameEvent) bool {
		return g.Timestamp > startMinTimestamp // && g.Timestamp < stopMinTimestamp
	})

	timestampedEvents := beam.ParDo(scope.Scope(""), &addTimestampFn{}, filteredStartTime)
	// debug.Head(scope, timestampedEvents, 10)
	debug.Printf(scope, "ts--- %+v", timestampedEvents)
	fixedWindowsTeam := beam.WindowInto(scope, window.NewFixedWindows(time.Minute), timestampedEvents)
	debug.Printf(scope, "%+v", fixedWindowsTeam)
	teamTotalScore := (&game.ExtractAndSumScore{Field: "team"}).Expand(scope, fixedWindowsTeam)
	debug.Printf(scope, "teamscore++++ %+v", teamTotalScore)
	formattedResults := beam.ParDo(scope, &FormatResult{}, teamTotalScore)
	debug.Printf(scope, "formatted==== %+v", formattedResults)
	// textio.Write(scope, "/tmp/hourly", formattedResults)

	if err := beamx.Run(context.Background(), pipeline); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}

type addTimestampFn struct {
}

func (f *addTimestampFn) ProcessElement(g game.GameEvent) (beam.EventTime, game.GameEvent) {
	return beam.EventTime(g.Timestamp), g
}

type FormatResult struct {
}

func (fr *FormatResult) ProcessElement(ctx context.Context, ts beam.EventTime, team string, score int64, emit func(string)) {
	formatted := fmt.Sprintf("start: %v, team: %s, total_score: %v", ts, team, score)
	emit(formatted)
}
