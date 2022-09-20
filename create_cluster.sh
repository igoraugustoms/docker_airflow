#!/bin/bash
eksctl create cluster --name=igor \
--managed \
--instance-types=m5.large \
--spot \
--nodes-min=2 --nodes-max=4 \
--region=us-east-2 \
--alb-ingress-access \
--node-private-networking \
--full-ecr-access \
--nodegroup-name=ng-igor