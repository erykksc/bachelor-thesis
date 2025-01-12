expose.pdf: expose.tex resources
	latexmk -auxdir=aux -pdf expose.tex

resources: benchmark-architecture.mermaid
	mkdir -p ./resources
	mmdc -i ./benchmark-architecture.mermaid -o ./resources/benchmark-architecture.png

clean:
	latexmk -c
	rm -f ./expose.pdf
	rm -rf ./aux
	rm -rf ./resources

