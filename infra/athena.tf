resource "aws_athena_workgroup" "orderflow" {
  name = "${var.project_name}-workgroup"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${var.s3_bucket_name}/athena-results/"
    }
  }
}

resource "aws_glue_catalog_database" "orderflow" {
  name = "${var.project_name}_db"
}

resource "aws_glue_catalog_table" "imbalance" {
  name          = "imbalance"
  database_name = aws_glue_catalog_database.orderflow.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL                     = "TRUE"
    "skip.header.line.count"     = "0"
    "projection.enabled"         = "true"
    "projection.dt_hour.type"    = "date"
    "projection.dt_hour.range"   = "2026-06-01--00,NOW"
    "projection.dt_hour.format"  = "yyyy-MM-dd--HH"
    "projection.dt_hour.interval" = "1"
    "projection.dt_hour.interval.unit" = "HOURS"
    "storage.location.template"  = "s3://${var.s3_bucket_name}/metrics/imbalance/$${dt_hour}/"
  }

  storage_descriptor {
    location      = "s3://${var.s3_bucket_name}/metrics/imbalance/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
      parameters = {
        "field.delim" = ","
      }
    }

    columns {
      name = "symbol"
      type = "string"
    }
    columns {
      name = "window_start"
      type = "bigint"
    }
    columns {
      name = "window_end"
      type = "bigint"
    }
    columns {
      name = "imbalance"
      type = "double"
    }
    columns {
      name = "weighted_bids"
      type = "double"
    }
    columns {
      name = "weighted_asks"
      type = "double"
    }
  }

  partition_keys {
    name = "dt_hour"
    type = "string"
  }
}