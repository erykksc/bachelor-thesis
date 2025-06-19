# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview
This repository contains a bachelor thesis project evaluating the vertical and horizontal read and write scalability of MobilityDB. The project includes benchmarking infrastructure, thesis documentation, and cloud deployment configurations.

## Development Environment
This project uses Nix flakes for development environment management:
- `nix develop` - Enter development shell with all required tools
- Required tools include: azure-cli, kubectl, kubernetes-helm, opentofu, tectonic (LaTeX)

## Key Commands

### Thesis (LaTeX)
Located in `thesis/` directory:
- `make thesis.pdf` - Build the thesis PDF using latexmk
- `make clean` - Clean LaTeX build artifacts
- Use `latexmk -pdf thesis.tex` for manual builds

### Infrastructure Deployment
- **Terraform**: Infrastructure as code for Azure AKS deployment in `terraform/`
  - `terraform init && terraform plan && terraform apply` - Deploy AKS cluster
- **Helm**: Kubernetes application deployment using chart in `chart/mobilitydbc-benchmark/`
  - `helm install <name> ./chart/mobilitydbc-benchmark` - Deploy benchmark application

### Benchmarking
- Docker configurations in `benchmark/` for MobilityDB and CrateDB
- Kubernetes-based distributed benchmarking via Helm chart

## Architecture
The project consists of three main components:

1. **Benchmarking Infrastructure**: 
   - Dockerized database setups (MobilityDB, CrateDB)
   - Kubernetes-native distributed benchmark execution
   - Coordinator-manager-worker architecture for distributed testing

2. **Cloud Deployment**:
   - Terraform configurations for Azure AKS clusters
   - Helm charts for application deployment
   - Support for horizontal scaling via worker replicas

3. **Thesis Documentation**:
   - LaTeX-based thesis using TU Berlin template
   - Chapters covering introduction, background, benchmark design, evaluation, related work, conclusion
   - Vector graphics in PDF format stored in `fig/` directory

## File Structure Notes
- `thesis/` - LaTeX thesis files with TU Berlin template
- `benchmark/` - Docker configurations for database benchmarking
- `chart/` - Helm chart for Kubernetes deployment
- `terraform/` - Infrastructure as code for cloud deployment
- `expose/` - Project proposal documentation (German academic requirement)
- `research/` - Academic papers and research materials