# Market Article Harvester Engine 🚀

Dedicated full-text financial article extraction engine running autonomously 24×7 on GitHub Actions cloud runners.

## Responsibilities:
- Reads un-opened URLs (`status: "discovered"`) from [`market-intelligence-vault`](https://github.com/parshant65-lang/market-intelligence-vault).
- Processes 100-URL batches across 9 categories using 20 parallel ThreadPool workers.
- Extracts 100% full-text paragraphs and scores polarity/sentiment.
- Partitions clean article records into date-stamped files in private vault.
- Commits and pushes every 30 minutes.
