#!/bin/bash

# Generate protobuf files
python -m grpc_tools.protoc \
    -I. \
    --python_out=. \
    --grpc_python_out=. \
    --pyi_out=. \
    database.proto

# Fix imports in generated files
sed -i 's/import database_pb2/from . import database_pb2/' database_pb2_grpc.py

# Move generated files to the correct directory
mv database_pb2.py database_pb2.pyi database_pb2_grpc.py ../proto/ 