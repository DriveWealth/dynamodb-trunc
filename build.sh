go vet -json -composites=false ./...

env GOOS=linux GOARCH=amd64 go build -o ./bins/dtrunc
echo "Built dtrunc for Linux"
env GOOS=darwin GOARCH=amd64 go build -o ./bins/mtrunc
echo "Built dtrunc for Darwin"
env GOOS=windows GOARCH=amd64 go build -o ./bins/dtrunc.exe
echo "Built dtrunc for Windows"
