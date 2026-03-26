<h1 align="center">Osmanthus</h1>
<h3 align="center" style="font-size: 20px; margin-bottom: 4px">Modernize. Organize. Osmanthus.</h3>
<p align="center">
  <a style="padding: 4px;" href="https://github.com/user177013/lilac">
    <span style="margin-right: 4px; font-size: 12px">🔗</span> <span style="font-size: 14px">Maintained fork of the archived Lilac project.</span>
  </a>
  <br/><br/>
    <a href="https://github.com/user177013/lilac/blob/main/LICENSE">
          <img alt="License Apache 2.0" src="https://img.shields.io/badge/License-Apache 2.0-blue.svg?style=flat&color=89bd9e" height="20" width="auto">
    </a>
    <br/>
    <a href="https://github.com/user177013/lilac">
      <img src="https://img.shields.io/github/stars/user177013/lilac?style=social" />
    </a>
</p>

Osmanthus is a production-ready fork of the archived [Lilac](https://github.com/lilacai/lilac) project. It is designed for exploration, curation, and quality control of datasets for LLMs, with a focus on modern embedding infrastructure and Windows stability.

Osmanthus continues the mission of providing "Better data, better AI" by maintaining the core registry-based signal architecture while decoupling from defunct hosted services.

## ✨ Key Features in this Fork

- **Modern GGUF Support**: Enhanced `llama-cpp-python` integration for state-of-the-art GGUF embeddings.
- **Independent Identity**: Full decoupling from the defunct "Lilac Garden" infrastructure.
- **Windows Optimized**: Critical stability fixes for high-performance embedding pipelines on Windows systems.
- **Botanical UI**: A premium, high-density design system focusing on "Cockpit Mode" utility.

## Why Osmanthus?

- **Interactive Exploration**: Search, filter, cluster, and annotate your data with an LLM-powered interface.
- **On-Device Performance**: Runs entirely on your local machine using open-source LLMs.
- **Data Hygiene**: Detect PII, remove duplicates, and analyze text statistics to lower training costs.
- **Centralized Insights**: Understand how your data evolves across the entire ML lifecycle.

<img alt="Osmanthus UI" src="docs/_static/dataset/dataset_cluster_view.png">

## 🔥 Getting Started

### 💻 Install

```sh
# Install directly from the fork repository
pip install git+https://github.com/user177013/lilac.git
```

### 🌐 Start the Server

Start the Osmanthus webserver using the new CLI:

```sh
osmanthus start ~/my_project
```

Or from Python:

```python
import osmanthus as osman
osman.start_server(project_dir='~/my_project')
```

The server will be available at http://localhost:5432/.

## 📊 Documentation

For detailed guides on loading datasets from HuggingFace, Parquet, JSON, and more, please refer to the [docs/](docs/) folder.

## ⚖️ License

Osmanthus is licensed under the [Apache License, Version 2.0](LICENSE). 
This project is an independent fork and is not affiliated with the original Lilac AI Inc. team.

---
*Created with focus on performance and independence.*
