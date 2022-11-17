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


resource "aws_glue_crawler" "DIM_TRACK" {
  database_name = aws_glue_catalog_database.db.name
  name          = "s3_crawler_dim_track"
  role          = aws_iam_role.glue_role.arn

  s3_target {
    path = "s3://datalake-igti-igor/spotify/exp/dim_track/"
  }

  configuration = <<EOF
{
   "Version": 1.0,
   "Grouping": {
      "TableGroupingPolicy": "CombineCompatibleSchemas" }
}
EOF

}

resource "aws_glue_crawler" "F_ARTIST" {
  database_name = aws_glue_catalog_database.db.name
  name          = "s3_crawler_f_artist"
  role          = aws_iam_role.glue_role.arn

  s3_target {
    path = "s3://datalake-igti-igor/spotify/exp/f_artist/"
  }

  configuration = <<EOF
{
   "Version": 1.0,
   "Grouping": {
      "TableGroupingPolicy": "CombineCompatibleSchemas" }
}
EOF

}


resource "aws_glue_crawler" "F_GENRE" {
  database_name = aws_glue_catalog_database.db.name
  name          = "s3_crawler_f_genre"
  role          = aws_iam_role.glue_role.arn

  s3_target {
    path = "s3://datalake-igti-igor/spotify/exp/f_genre/"
  }

  configuration = <<EOF
{
   "Version": 1.0,
   "Grouping": {
      "TableGroupingPolicy": "CombineCompatibleSchemas" }
}
EOF

}



resource "aws_glue_crawler" "F_RANKING" {
  database_name = aws_glue_catalog_database.db.name
  name          = "s3_crawler_f_ranking"
  role          = aws_iam_role.glue_role.arn

  s3_target {
    path = "s3://datalake-igti-igor/spotify/exp/f_ranking/"
  }

  configuration = <<EOF
{
   "Version": 1.0,
   "Grouping": {
      "TableGroupingPolicy": "CombineCompatibleSchemas" }
}
EOF

}



resource "aws_glue_crawler" "F_TRACK_STATISTICS" {
  database_name = aws_glue_catalog_database.db.name
  name          = "s3_crawler_f_track_statistics"
  role          = aws_iam_role.glue_role.arn

  s3_target {
    path = "s3://datalake-igti-igor/spotify/exp/f_track_statistics/"
  }

  configuration = <<EOF
{
   "Version": 1.0,
   "Grouping": {
      "TableGroupingPolicy": "CombineCompatibleSchemas" }
}
EOF

}