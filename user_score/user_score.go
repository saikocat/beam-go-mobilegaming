package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/apache/beam/sdks/go/examples/complete/game"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
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
		"broken",
		"user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224",
		"user0_AzureBilby,AzureBilby,8,1447719060000,2015-11-16 16:11:03.955",
		"user9_AzureBilby,AzureBilby,5,1447719060000,2015-11-16 16:11:03.959",
		"user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224"})

	// Parse csv input into Game Event
	parseEventFn := &game.ParseEventFn{Sep: ",", NumFields: 5, NumParseErrors: &numParseErrors}
	parsedEvents := beam.ParDo(scope.Scope("ParseEventFn"), parseEventFn, lines)
	// Extract and sum username/score pairs from the event data.
	extractAndSumScoreFn := &game.ExtractAndSumScore{Field: "user"}
	userTotalScore := extractAndSumScoreFn.Expand(scope.Scope("ExtractAndSumScore"), parsedEvents)
	// Format the result
	formattedResults := beam.ParDo(scope, &FormatResult{}, userTotalScore)
	// Finally write the formatted results to text file
	textio.Write(scope, *output, formattedResults)

	if err := beamx.Run(context.Background(), pipeline); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
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
