# Cleanup resources before VPC destruction
# Handles ALBs, Security Groups, and other resources created by Kubernetes

resource "null_resource" "cleanup_k8s_resources" {
  triggers = {
    vpc_id  = module.vpc.vpc_id
    region  = var.aws_region
    cluster = var.cluster_name
  }

  provisioner "local-exec" {
    when    = destroy
    command = <<-EOT
      Write-Host "=== Cleaning up Kubernetes-created AWS resources ===" -ForegroundColor Yellow
      
      # Delete load balancers in the VPC
      Write-Host "Deleting Load Balancers..."
      $lbs = aws elbv2 describe-load-balancers --query "LoadBalancers[?VpcId=='${self.triggers.vpc_id}'].LoadBalancerArn" --output text --region ${self.triggers.region} 2>$null
      if ($lbs -and $lbs -ne "") {
        foreach ($arn in $lbs.Split()) {
          if ($arn -and $arn -ne "") {
            Write-Host "  Deleting ALB: $arn"
            aws elbv2 delete-load-balancer --load-balancer-arn $arn --region ${self.triggers.region} 2>$null
          }
        }
        Write-Host "Waiting 90s for ALB cleanup..."
        Start-Sleep -Seconds 90
      }
      
      # Delete target groups
      Write-Host "Deleting Target Groups..."
      $tgs = aws elbv2 describe-target-groups --query "TargetGroups[?VpcId=='${self.triggers.vpc_id}'].TargetGroupArn" --output text --region ${self.triggers.region} 2>$null
      if ($tgs -and $tgs -ne "") {
        foreach ($arn in $tgs.Split()) {
          if ($arn -and $arn -ne "") {
            Write-Host "  Deleting TG: $arn"
            aws elbv2 delete-target-group --target-group-arn $arn --region ${self.triggers.region} 2>$null
          }
        }
      }
      
      # Delete security group rules that reference other SGs (to break circular dependencies)
      Write-Host "Cleaning Security Group rules..."
      $sgs = aws ec2 describe-security-groups --filters "Name=vpc-id,Values=${self.triggers.vpc_id}" --query "SecurityGroups[?GroupName!='default'].GroupId" --output text --region ${self.triggers.region} 2>$null
      if ($sgs -and $sgs -ne "") {
        foreach ($sg in $sgs.Split()) {
          if ($sg -and $sg -ne "") {
            # Get rules that reference other security groups
            $rules = aws ec2 describe-security-group-rules --filters "Name=group-id,Values=$sg" --query "SecurityGroupRules[?ReferencedGroupInfo!=null].SecurityGroupRuleId" --output text --region ${self.triggers.region} 2>$null
            if ($rules -and $rules -ne "") {
              Write-Host "  Revoking rules from $sg..."
              aws ec2 revoke-security-group-ingress --group-id $sg --security-group-rule-ids $rules --region ${self.triggers.region} 2>$null
            }
          }
        }
      }
      
      # Delete security groups (except default)
      Write-Host "Deleting Security Groups..."
      if ($sgs -and $sgs -ne "") {
        foreach ($sg in $sgs.Split()) {
          if ($sg -and $sg -ne "") {
            Write-Host "  Deleting SG: $sg"
            aws ec2 delete-security-group --group-id $sg --region ${self.triggers.region} 2>$null
          }
        }
      }
      
      # Release unassociated EIPs
      Write-Host "Releasing Elastic IPs..."
      $eips = aws ec2 describe-addresses --query "Addresses[?AssociationId==null].AllocationId" --output text --region ${self.triggers.region} 2>$null
      if ($eips -and $eips -ne "") {
        foreach ($alloc in $eips.Split()) {
          if ($alloc -and $alloc -ne "" -and $alloc -ne "None") {
            Write-Host "  Releasing EIP: $alloc"
            aws ec2 release-address --allocation-id $alloc --region ${self.triggers.region} 2>$null
          }
        }
      }
      
      # Delete NAT Gateways
      Write-Host "Deleting NAT Gateways..."
      $nats = aws ec2 describe-nat-gateways --filter "Name=vpc-id,Values=${self.triggers.vpc_id}" "Name=state,Values=available,pending" --query "NatGateways[*].NatGatewayId" --output text --region ${self.triggers.region} 2>$null
      if ($nats -and $nats -ne "") {
        foreach ($nat in $nats.Split()) {
          if ($nat -and $nat -ne "") {
            Write-Host "  Deleting NAT: $nat"
            aws ec2 delete-nat-gateway --nat-gateway-id $nat --region ${self.triggers.region} 2>$null
          }
        }
        Write-Host "Waiting 60s for NAT Gateway cleanup..."
        Start-Sleep -Seconds 60
      }
      
      Write-Host "=== Cleanup complete ===" -ForegroundColor Green
    EOT
    interpreter = ["powershell", "-Command"]
  }

  #depends_on = [module.eks]
}
