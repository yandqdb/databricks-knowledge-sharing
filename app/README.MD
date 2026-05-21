# Prompt Evaluation Notebook

A worked example of an end-to-end **prompt evaluation pipeline** for the Anthropic Claude API — useful when you need to compare prompt variants, score outputs, and track regression on a fixed test set.

## Contents

| File | Purpose |
|---|---|
| `001_prompting.ipynb` | Self-contained notebook with a `PromptEvaluator` class — generates a synthetic test dataset from a task description + input spec, renders the prompt across the dataset with controlled concurrency, scores responses, and emits an HTML report (pass rate, average score, per-test detail). |

## What it shows

- Anthropic SDK usage (`from anthropic import Anthropic`) with model `claude-haiku-4-5`
- Template rendering with `{placeholder}` substitution
- Concurrent evaluation with a configurable `max_concurrent_tasks` to respect rate limits
- Synthetic dataset generation from a task description and input schema
- HTML report builder for sharing evaluation results

## Running it

```bash
pip install anthropic python-dotenv
echo "ANTHROPIC_API_KEY=sk-ant-..." > .env
jupyter notebook 001_prompting.ipynb
```

Then fill in the `task_description` and `prompt_inputs_spec` in the `generate_dataset(...)` call and run all cells.
