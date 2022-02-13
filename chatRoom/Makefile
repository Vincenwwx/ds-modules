# Path for saving the generated CAs
CONFIG_PATH=${HOME}/.proglog/

.PHONY: init, compile, test

# delete the old CAs and folder
init:
	mkdir -p ${CONFIG_PATH}

# generate certificates
gencert:
	cfssl gencert -initca test/ca-csr.json | cfssljson -bare ca

	cfssl gencert \
			-ca=ca.pem \
			-ca-key=ca-key.pem \
			-config=test/ca-config.json \
			-profile=server \
			test/server-csr.json | cfssljson -bare server
	cfssl gencert \
			-ca=ca.pem \
			-ca-key=ca-key.pem \
			-config=test/ca-config.json \
			-profile=client \
			-cn="root" \
			test/client-csr.json | cfssljson -bare root-client
	cfssl gencert \
			-ca=ca.pem \
			-ca-key=ca-key.pem \
			-config=test/ca-config.json \
			-profile=client \
			-cn="nobody" \
			test/client-csr.json | cfssljson -bare nobody-client

	mv *.pem *.csr ${CONFIG_PATH}

# Protobuf compile
compile:
	@protoc api/v1/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.
	@echo "Successfully compile protobuf!"

# Test if all modules work normally
# $(CONFIG_PATH)/model.conf:
copy_acl:
	@cp test/model.conf $(CONFIG_PATH)/model.conf
	@cp test/policy.csv $(CONFIG_PATH)/policy.csv

# $(CONFIG_PATH)/policy.csv:

# test: $(CONFIG_PATH)/policy.csv $(CONFIG_PATH)/model.conf
test: copy_acl
	go test -race ./...
