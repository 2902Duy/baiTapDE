from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

sp = SparkSession.builder \
    .appName("DE-ETL-102")\
    .master("local[*]")\
    .config("spark.executor.memory","4g")\
    .getOrCreate()

schema_user = StructField("user",StructType([
                    StructField("login",StringType(),True),
                    StructField("id",IntegerType(),True),
                    StructField("avatar_url",StringType(),True),
                    StructField("gravatar_id",StringType(),True),
                    StructField("url",StringType(),True),
                    StructField("html_url",StringType(),True),
                    StructField("follower_url",StringType(),True),
                    StructField("following_url",StringType(),True),
                    StructField("gists_url",StringType(),True),
                    StructField("starred_url",StringType(),True),
                    StructField("subscriptions_url",StringType(),True),
                    StructField("organizations_url",StringType(),True),
                    StructField("repos_url",StringType(),True),
                    StructField("events_url",StringType(),True),
                    StructField("received_events_url",StringType(),True),
                    StructField("type",StringType(),True),
                    StructField("site_admin",BooleanType(),True),
            ]),True)

schema_owner= StructField("owner",StructType([
    StructField("login",StringType(),True),
    StructField("id",IntegerType(),True),
    StructField("avatar_url",StringType(),True),
    StructField("gravatar_id",StringType(),True),
    StructField("url",StringType(),True),
    StructField("html_url",StringType(),True),
    StructField("follower_url",StringType(),True),
    StructField("following_url",StringType(),True),
    StructField("gists_url",StringType(),True),
    StructField("starred_url",StringType(),True),
    StructField("subscriptions_url",StringType(),True),
    StructField("organizations_url",StringType(),True),
    StructField("repos_url",StringType(),True),
    StructField("events_url",StringType(),True),
    StructField("received_events_url",StringType(),True),
    StructField("type",StringType(),True),
    StructField("site_admin",BooleanType(),True),
                ]),True)

schema_repo=StructField("repo",StructType([
    StructField("id",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("full_name",StringType(),True),
    schema_owner,
    StructField("private",BooleanType(),True),
    StructField("html_url", StringType(), True),
    StructField("description", StringType(), True),
    StructField("fork", BooleanType(), True),
    StructField("url", StringType(), True),
    StructField("forks_url", StringType(), True),
    StructField("keys_url", StringType(), True),
    StructField("collaborators_url", StringType(), True),
    StructField("teams_url", StringType(), True),
    StructField("hooks_url", StringType(), True),
    StructField("issue_events_url", StringType(), True),
    StructField("events_url", StringType(), True),
    StructField("assignees_url", StringType(), True),
    StructField("branches_url", StringType(), True),
    StructField("tags_url", StringType(), True),
    StructField("blobs_url", StringType(), True),
    StructField("git_tags_url", StringType(), True),
    StructField("git_refs_url", StringType(), True),
    StructField("trees_url", StringType(), True),
    StructField("statuses_url", StringType(), True),
    StructField("languages_url", StringType(), True),
    StructField("stargazers_url", StringType(), True),
    StructField("contributors_url", StringType(), True),
    StructField("subscribers_url", StringType(), True),
    StructField("subscription_url", StringType(), True),
    StructField("commits_url", StringType(), True),
    StructField("git_commits_url", StringType(), True),
    StructField("comments_url", StringType(), True),
    StructField("issue_comment_url", StringType(), True),
    StructField("contents_url", StringType(), True),
    StructField("compare_url", StringType(), True),
    StructField("archive_url", StringType(), True),
    StructField("downloads_url", StringType(), True),
    StructField("issues_url", StringType(), True),
    StructField("pulls_url", StringType(), True),
    StructField("milestones_url", StringType(), True),
    StructField("notifications_url", StringType(), True),
    StructField("labels_url", StringType(), True),
    StructField("releases_url", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("pushed_at", StringType(), True),
    StructField("git_url", StringType(), True),
    StructField("ssh_url", StringType(), True),
    StructField("clone_url", StringType(), True),
    StructField("svn_url", StringType(), True),
    StructField("homepage", StringType(), True),
    StructField("size", IntegerType(), True),
    StructField("stargazers_count", IntegerType(), True),
    StructField("watchers_count", IntegerType(), True),
    StructField("language", StringType(), True),
    StructField("has_issues", BooleanType(), True),
    StructField("has_downloads", BooleanType(), True),
    StructField("has_wiki", BooleanType(), True),
    StructField("has_pages", BooleanType(), True),
    StructField("forks_count", IntegerType(), True),
    StructField("mirror_url", NullType(), True),
    StructField("open_issues_count", IntegerType(), True),
    StructField("forks", IntegerType(), True),
    StructField("open_issues", IntegerType(), True),
    StructField("watchers", IntegerType(), True),
    StructField("default_branch", StringType(), True),
            ]),True)

schema_org =StructField("org",StructType([
        StructField("id",IntegerType(),True),
        StructField("login",StringType(),True),
        StructField("gravatar_id",StringType(),True),
        StructField("url",StringType(),True),
        StructField("gravatar_url",StringType(),True)
]),True)
schema_json = StructType([
    StructField("id",StringType(),True),
    StructField("type",StringType(),True),
    StructField("actor",StructType([
        StructField("id",IntegerType(),True),
        StructField("login",StringType(),True),
        StructField("gravatar",StringType(),True),
        StructField("url",StringType(),True),
        StructField("gravatar_url",StringType(),True)
    ]),True),
    StructField("repo",StructType([
        StructField("id",IntegerType(),True),
        StructField("name",StringType(),True),
        StructField("url",StringType(),True)
    ]),True),
    StructField("payload",StructType([
        StructField("action",StringType(),True),
        StructField("comment",StructType([
            StructField("url",StringType(),True),
            StructField("id",IntegerType(),True),
            StructField("diff_hunk",StringType(),True),
            StructField("path",StringType(),True),
            StructField("position",IntegerType(),True),
            StructField("original_position",IntegerType(),True),
            StructField("commit_id",StringType(),True),
            StructField("original_commit_id",StringType(),True),
            schema_user,
            StructField("body",StringType(),True),
            StructField("created_at",StringType(),True),
            StructField("updated_at",StringType(),True),
            StructField("html_url",StringType(),True),
            StructField("pull_request_url",StringType(),True),
            StructField("_links",StructType([
                StructField("self",StructType([
                    StructField("href",StringType(),True)
                ]),True),
                StructField("html",StructType([
                    StructField("href",StringType(),True)
                ]),True),
                StructField("pull_request",StructType([
                    StructField("href",StringType(),True)
                ]),True),
            ]),True),
        ]),True),
        StructField("pull_request",StructType([
            StructField("url",StringType(),True),
            StructField("id",IntegerType(),True),
            StructField("html_url", StringType(), True),
            StructField("diff_url", StringType(), True),
            StructField("patch_url", StringType(), True),
            StructField("issue_url", StringType(), True),
            StructField("number",IntegerType(),True),
            StructField("state", StringType(), True),
            StructField("locked", BooleanType(), True),
            StructField("title", StringType(), True),
            schema_user,
            StructField("body", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("closed_at",NullType(),True),
            StructField("merged_at", NullType(), True),
            StructField("merge_commit_sha", StringType(), True),
            StructField("assignee", NullType(), True),
            StructField("milestone", NullType(), True),
            StructField("commits_url", StringType(), True),
            StructField("review_comments_url", StringType(), True),
            StructField("review_comment_url", StringType(), True),
            StructField("comments_url", StringType(), True),
            StructField("statuses_url", StringType(), True),
            StructField("head",StructType([
                StructField("label", StringType(), True),
                StructField("ref", StringType(), True),
                StructField("sha", StringType(), True),
                schema_user,
                schema_repo,
            ]),True),
            StructField("base",StructType([
                StructField("label",StringType(),True),
                StructField("ref",StringType(),True),
                StructField("sha",StringType(),True),
                schema_user,
                schema_repo
            ]),True),
            StructField("_links",StructType([
                StructField("self",StructType([
                    StructField("href",StringType(),True)
                ]),True),
                StructField("html",StructType([
                    StructField("href",StringType(),True)
                ]),True),
                StructField("issue",StructType([
                    StructField("href",StringType(),True)
                ]),True),
                StructField("comments",StructType([
                    StructField("href",StringType(),True)
                ]),True),
                StructField("review_comments",StructType([
                    StructField("href",StringType(),True)
                ]),True),
                StructField("review_comment",StructType([
                    StructField("href",StringType(),True)
                ]),True),
                StructField("commits",StructType([
                    StructField("href",StringType(),True)
                ]),True),
                StructField("statuses",StructType([
                    StructField("href",StringType(),True)
                ]),True),
            ]),True),
        ]),True),
    StructField("public",BooleanType(),True),
    StructField("created_at", StringType(), True),
    schema_org
    ]),True)
])
jsonData = sp.read.schema(schema_json).json("F:\DE\sparkRDD/2015-03-01-17.json")
# jsonData.show(truncate=False)
# jsonData.select(
#     col("id"),
#     col("payload.comment.user.id").alias("actor_id")
# ).show()

# jsonData.printSchema()
# jsonData.selectExpr(
#     "id",
#     "actor.id as actor_id",
#     "payload.comment.user.id as payload_comment_user_id"
# ).show()


# jsonData.selectExpr(
#     "id",
#     "upper(actor.login) as actor_name",
#     "payload.comment.user.id as payload_comment_user_id",
#     "CASE WHEN payload.comment.user.id >9000 THEN 'PowerUser' ELSE 'Normal' END as user_type"
# ).show()

print(jsonData.filter("payload.comment.user.id>5000") \
    .select("id", "payload.comment.user.id") \
    .count())

