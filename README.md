# beam-go-mobilegaming
Porting the complete examples (mobile gaming) of Apache Beam for golang. 

# User Score
```
$ go run user_score/user_score.go --output=/tmp/beam-result
2019/08/18 22:08:37 Executing pipeline with the direct runner.
...
2019/08/18 22:08:37 Writing to /tmp/beam-results
2019/08/18 22:08:37 Bundle: "plan" - PTransformID: "game.ParseEventFn"
2019/08/18 22:08:37     main.ParseErrors - value: 1

```

# Hourly Team Score
