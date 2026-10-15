PY ?= python
RUN = PYTHONPATH=src $(PY)

.PHONY: test demo benchmark live crash kafka-e2e report clean

test:  ## unit tests (detectors + exactly-once sinks)
	$(PY) -m pytest -q

demo:  ## simulate 30 min of traffic, run the stream over it, score it
	$(RUN) -m fraudstream.simulator --minutes 30
	$(RUN) -m fraudstream.job --available-now --max-files-per-trigger 4
	$(RUN) -m fraudstream.evaluate --min-recall 0.9

benchmark:  ## score on three seeds never used in development -> docs/benchmark.json
	$(RUN) scripts/benchmark.py

live:  ## job running while traffic arrives; measures latency -> docs/live_run.json
	$(RUN) scripts/live_run.py

crash:  ## kill -9 the job mid-stream twice, restart, prove nothing lost or doubled -> docs/crash_test.json
	$(RUN) scripts/crash_test.py

kafka-e2e:  ## same pipeline on a real Kafka broker (docker compose up -d kafka first)
	$(RUN) scripts/kafka_e2e.py --bootstrap localhost:9092

report:  ## charts for the README from the docs/*.json results
	$(RUN) scripts/report.py

clean:
	rm -rf data
