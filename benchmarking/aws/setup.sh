#!/usr/bin/env bash
# setup.sh — one-time AWS setup for ibex benchmarking.
#
# Creates:
#   - S3 bucket           (stores results; objects expire after 30 days)
#   - IAM policy          (PutObject on that bucket only)
#   - IAM role + profile  (attached to benchmark instances)
#   - EC2 security group  (outbound-only; no inbound)
#   - benchmarking/aws/.config (saves bucket name for run.sh)
#
# Usage:
#   AWS_REGION=us-east-1 ./benchmarking/aws/setup.sh
#   AWS_REGION=us-east-1 S3_BUCKET=my-bucket ./benchmarking/aws/setup.sh

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/.config"

# ── S3 bucket ─────────────────────────────────────────────────────────────────
if [[ -z "${S3_BUCKET:-}" ]]; then
    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    S3_BUCKET="ibex-bench-${ACCOUNT_ID}"
fi

echo "Region : $REGION"
echo "Bucket : $S3_BUCKET"
echo ""

if aws s3api head-bucket --bucket "$S3_BUCKET" --region "$REGION" 2>/dev/null; then
    echo "✓ S3 bucket already exists"
else
    if [[ "$REGION" == "us-east-1" ]]; then
        aws s3api create-bucket --bucket "$S3_BUCKET" --region "$REGION"
    else
        aws s3api create-bucket --bucket "$S3_BUCKET" --region "$REGION" \
            --create-bucket-configuration "LocationConstraint=$REGION"
    fi
    echo "✓ Created s3://$S3_BUCKET"
fi

# Lifecycle: expire objects after 30 days
aws s3api put-bucket-lifecycle-configuration \
    --bucket "$S3_BUCKET" \
    --lifecycle-configuration '{
        "Rules": [{
            "ID": "expire-benchmarks",
            "Status": "Enabled",
            "Prefix": "benchmarks/",
            "Expiration": {"Days": 30}
        }]
    }' 2>/dev/null && echo "✓ Lifecycle rule set (objects expire after 30 days)"

# ── IAM policy ────────────────────────────────────────────────────────────────
POLICY_NAME="ibex-bench-s3"
POLICY_ARN=$(aws iam list-policies \
    --query "Policies[?PolicyName=='${POLICY_NAME}'].Arn" \
    --output text)

if [[ -n "$POLICY_ARN" ]]; then
    echo "✓ IAM policy already exists"
else
    POLICY_ARN=$(aws iam create-policy \
        --policy-name "$POLICY_NAME" \
        --policy-document "{
            \"Version\": \"2012-10-17\",
            \"Statement\": [{
                \"Effect\": \"Allow\",
                \"Action\": [\"s3:PutObject\"],
                \"Resource\": \"arn:aws:s3:::${S3_BUCKET}/benchmarks/*\"
            }]
        }" \
        --query "Policy.Arn" --output text)
    echo "✓ Created IAM policy $POLICY_ARN"
fi

# ── IAM role + instance profile ───────────────────────────────────────────────
ROLE_NAME="ibex-bench"
if aws iam get-role --role-name "$ROLE_NAME" &>/dev/null; then
    echo "✓ IAM role already exists"
else
    aws iam create-role \
        --role-name "$ROLE_NAME" \
        --assume-role-policy-document '{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "ec2.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }' > /dev/null
    aws iam attach-role-policy --role-name "$ROLE_NAME" --policy-arn "$POLICY_ARN"
    echo "✓ Created IAM role $ROLE_NAME"
fi

if aws iam get-instance-profile --instance-profile-name "$ROLE_NAME" &>/dev/null; then
    echo "✓ Instance profile already exists"
else
    aws iam create-instance-profile --instance-profile-name "$ROLE_NAME" > /dev/null
    aws iam add-role-to-instance-profile \
        --instance-profile-name "$ROLE_NAME" --role-name "$ROLE_NAME"
    echo "✓ Created instance profile $ROLE_NAME"
    echo "  (waiting 10s for IAM propagation...)"
    sleep 10
fi

# ── Security group ────────────────────────────────────────────────────────────
SG_NAME="ibex-bench"
SG_ID=$(aws ec2 describe-security-groups \
    --region "$REGION" \
    --filters "Name=group-name,Values=$SG_NAME" \
    --query "SecurityGroups[0].GroupId" \
    --output text 2>/dev/null)

if [[ "$SG_ID" != "None" && -n "$SG_ID" ]]; then
    echo "✓ Security group already exists ($SG_ID)"
else
    SG_ID=$(aws ec2 create-security-group \
        --region "$REGION" \
        --group-name "$SG_NAME" \
        --description "ibex benchmark instances — outbound only" \
        --query "GroupId" --output text)
    # Remove default allow-all-outbound rule and replace with HTTPS + HTTP only
    # (actually outbound-all is fine and simpler; no inbound rules means deny-all inbound)
    echo "✓ Created security group $SG_ID (no inbound, all outbound)"
fi

# ── Save config ───────────────────────────────────────────────────────────────
cat > "$CONFIG_FILE" <<EOF
S3_BUCKET=${S3_BUCKET}
AWS_REGION=${REGION}
EOF
echo ""
echo "✓ Config saved to $CONFIG_FILE"
echo ""
echo "Setup complete. Run benchmarks with:"
echo "  ./benchmarking/aws/run.sh"
