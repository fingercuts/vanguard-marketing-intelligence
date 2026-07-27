.PHONY: setup generate pipeline dashboard test lint quality clean help

help:
	@echo "Vanguard Marketing Intelligence — Pipeline Management"
	@echo ""
	@echo "Available commands:"
	@echo "  setup          - Install dependencies"
	@echo "  generate       - Generate synthetic marketing data"
	@echo "  pipeline       - Run full Bronze → Silver → Gold pipeline"
	@echo "  dashboard      - Launch the Streamlit analytical dashboard"
	@echo "  quality        - Run data quality checks"
	@echo "  test           - Run Python unit and integration tests (pytest)"
	@echo "  lint           - Run code style checks (pre-commit)"
	@echo "  clean          - Remove generated databases and logs"

setup:
	pip install -r requirements.txt

generate:
	python src/generator/data_generator.py

pipeline:
	python src/pipeline/bronze.py
	python src/pipeline/silver.py
	python src/pipeline/gold.py

quality:
	python src/quality/data_quality.py

dashboard:
	streamlit run dashboard/Home.py

test:
	PYTHONPATH=. pytest tests/ -v

lint:
	pre-commit run --all-files

docker-up:
	docker-compose up --build

clean:
	@echo "Cleaning artifacts..."
	@rm -f db/*.duckdb db/*.wal db/*.tmp
	@rm -f logs/*.log
	@echo "Clean complete."

reset: clean setup
	@echo "Environment reset successful."
