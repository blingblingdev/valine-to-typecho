#!venv/bin python
import mysql.connector
import json
from datetime import datetime
import re

# The origin comments exported from Leancloud
_json_file_path = 'comments.json'

# The nick name of the owner of the blog
_author_name = 'Bear'

# Database information.
_db_host = '127.0.0.1'
_db_port = '3306'
_db_username = 'root'
_db_password = '123456'
_db_name = 'typecho'
_db_table_prefix = 'typecho_'

# The regex to find find the parent-child relationship of comments.
_re_url = r"(?<=href=\").+?(?=\")|(?<=href=\').+?(?=\')"

# The sql to insert comment.
_insert_comment_sql = 'insert into {}comments (cid, created, author, authorId, ownerId, mail, url, ip, agent, text, type, status, parent) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'.format(_db_table_prefix)

# The sql to refresh comment num of all posts.
_update_all_contents_comment_num_sql = 'update {}contents contents left join (select cid, count(1) as commentsNum from {}comments group by cid) comments on contents.cid = comments.cid set contents.commentsNum = if (comments.commentsNum is not null, comments.commentsNum, 0)'.format(_db_table_prefix, _db_table_prefix)

# Connect to the database first of all.
conn = mysql.connector.connect(host=_db_host, port=_db_port, user=_db_username, password=_db_password, database=_db_name)


def get_posts():
    """
    Get all posts from the database and make a dict with url and post id.

    For example, the slug is 'hello-world' and permanent link format is '/posts/{slug}/',
    and the post id of it is 10086 then a pair of '/posts/hello-world/':10086 should be
    append into the dict, after that the dict should be like:

        {'/posts/hello-world/': 10086}

    Input: Nothing.
    Output: A dict which key for the link of the post and value for the id of the post.
    """

    posts = {}
    cursor = conn.cursor()
    try:
        cursor = conn.cursor()
        cursor.execute('select cid, slug from m_contents where type=\'post\'')
        for row in cursor:
            cid = row[0]
            slug = row[1]
            posts['/posts/{}/'.format(slug)] = cid
        return posts
    finally:
        cursor.close()


def get_comments(file_path=''):
    """
    Get all comments from the json file exported from Leancloud.

    Input: The path of the json file.
    Output: A list of all the comments.
    """

    with open(file_path, 'r') as f:
        raw_comments = json.load(f)
    comments = []
    for raw_comment in raw_comments:
        comment = {
            'objectId': raw_comment['objectId'],
            'nick': raw_comment['nick'],
            'mail': raw_comment['mail'],
            'link': raw_comment['link'],
            'url': raw_comment['url'],
            'ua': raw_comment['ua'],
            'createdAt': int(datetime.strptime(raw_comment['createdAt'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(microsecond=0).timestamp()),
            'comment': raw_comment['comment']
        }
        comments.append(comment)
    return comments


def get_clear_comment(comment=''):
    """
    Remove the 'at' tag like
        <a class="at" href="#b9dff45f7e063b26e009f243">@Somebody </a> ,
    in the origin comment.

    Input: Origin comment.
    OutPut: Cleared Comment.
    """

    # Remove 'at' tag.
    c = re.compile('<a class="at"(.*?) href=("|\')(.*?)("|\')>(.*?)</a>')
    ret = c.sub('', comment)

    # Remove useless ' , '.
    c = re.compile('( , )')
    return c.sub('', ret)


def sync_comments(posts=None, comments=None):
    """
    This is the main part of the program, it will insert all the comment into the database.

    The parent-child relationship commented in the original file will also be inherited to
    the database. After all the work is completed, the number of comments in all texts will
    also be refreshed.

    Input: All posts and comments.
    Output: Nothing.
    """

    if posts is None:
        posts = {}
    if comments is None:
        comments = {}

    cursor = conn.cursor()
    object_id_to_comment_id = {}
    try:
        for comment in comments:
            if comment['url'] in posts:
                # Get post id from dict.
                cid = posts[comment['url']]

                # Whether the author in the origin comment is the owner of the blog.
                author_id = 0
                if comment['nick'] == _author_name:
                    author_id = 1

                # Whether this comment is son of another comment or not.
                # If yes, set parent_comment_id to its parent comment id.
                parent_comment_id = 0
                results = re.findall(_re_url, comment['comment'], re.I | re.S | re.M)
                for result in results:
                    if result.startswith('#'):
                        parent_object_id = result.replace('#', '')
                        if parent_object_id in object_id_to_comment_id:
                            parent_comment_id = object_id_to_comment_id[parent_object_id]

                # Remove the 'at' tag in the son comment.
                comment['comment'] = get_clear_comment(comment['comment'])

                # Put all elements in to a tuple.
                val = (cid, comment['createdAt'], comment['nick'], author_id, 1, comment['mail'], comment['link'], '127.0.0.1', comment['ua'], comment['comment'], 'comment', 'approved', parent_comment_id)

                # Do the comment insert job.
                cursor.execute(_insert_comment_sql, val)

                # Save the relation of object id and comment id.
                object_id_to_comment_id[comment['objectId']] = cursor.lastrowid

        # Refresh the comment num of all posts.
        cursor.execute(_update_all_contents_comment_num_sql)
    finally:
        conn.commit()
        cursor.close()


def main():
    """
    The Entrance of the entire program.
    """

    posts = get_posts()
    comments = get_comments(_json_file_path)
    sync_comments(posts, comments)


if __name__ == '__main__':
    main()
