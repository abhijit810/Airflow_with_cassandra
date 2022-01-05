from cassandra.cluster import Cluster
from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED
from cassandra.auth import PlainTextAuthProvider
import boto3
from cassandra_sigv4.auth import SigV4AuthProvider

#docker exec -it cassandra bash -c "cqlsh -u cassandra -p cassandra

ssl_context = SSLContext(PROTOCOL_TLSv1_2)
ssl_context.load_verify_locations('sf-class2-root.crt')
ssl_context.verify_mode = CERT_REQUIRED

# use this if you want to use Boto to set the session parameters.
boto_session = boto3.Session(aws_access_key_id="AKIAYOTRUKBAYOMI5OCU",
                             aws_secret_access_key="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",
                             region_name="ap-south-1")
auth_provider = SigV4AuthProvider(boto_session)

# Use this instead of the above line if you want to use the Default Credentials and not bother with a session.
# auth_provider = SigV4AuthProvider()

#cluster = Cluster(['cassandra.ap-south-1.amazonaws.com'], ssl_context=ssl_context, auth_provider=auth_provider,port=9142)

cluster = Cluster()
session = cluster.connect()
r = session.execute('select * from system_schema.keyspaces')
print(r.current_rows)