expose.pdf: expose.tex resources
	latexmk -auxdir=aux -pdf expose.tex

clean:
	latexmk -c
	rm -f ./expose.pdf
	rm -rf ./aux

