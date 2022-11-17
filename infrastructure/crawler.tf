resource "aws_glue_catalog_database" "db" {
  name = "tracks"
}

resource "aws_glue_crawler" "DIM_ARTIST" {
  database_name = aws_glue_catalog_database.db.name
  name          = "s3_crawler_dim_artist"
  role          = aws_iam_role.glue_role.arn

  s3_target {
    path = "s3://datalake-igti-igor/spotify/exp/dim_artist/"
  }

  configuration = <<EOF
{
   "Version": 1.0,
   "Grouping": {
      "TableGroupingPolicy": "CombineCompatibleSchemas" }
}
EOF

}