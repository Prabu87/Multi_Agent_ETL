resource "aws_cloudwatch_log_group" "etl_platform" {
  name              = "/aws/etl-platform/${var.environment}"
  retention_in_days = var.environment == "prod" ? 30 : 7
  
  tags = {
    Environment = var.environment
  }
}

resource "aws_cloudwatch_dashboard" "main" {
  dashboard_name = "${var.environment}-etl-platform"
  
  dashboard_body = jsonencode({
    widgets = [
      {
        type = "metric"
        properties = {
          metrics = [
            ["AWS/ECS", "CPUUtilization", { stat = "Average" }],
            [".", "MemoryUtilization", { stat = "Average" }]
          ]
          period = 300
          stat   = "Average"
          region = data.aws_region.current.name
          title  = "ECS Resource Utilization"
        }
      },
      {
        type = "log"
        properties = {
          query   = "SOURCE '${aws_cloudwatch_log_group.etl_platform.name}' | fields @timestamp, @message | sort @timestamp desc | limit 20"
          region  = data.aws_region.current.name
          title   = "Recent Logs"
        }
      }
    ]
  })
}

resource "aws_sns_topic" "alerts" {
  name = "${var.environment}-etl-alerts"
  
  tags = {
    Environment = var.environment
  }
}

resource "aws_cloudwatch_metric_alarm" "high_error_rate" {
  alarm_name          = "${var.environment}-etl-high-error-rate"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "Errors"
  namespace           = "ETL/Platform"
  period              = 300
  statistic           = "Sum"
  threshold           = 10
  alarm_description   = "This metric monitors ETL platform error rate"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  
  tags = {
    Environment = var.environment
  }
}

data "aws_region" "current" {}
