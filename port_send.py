import urllib2
import urllib # POST 요청은 data 값이 들어 있는가에 따라 요청 방식이 달라진다.
data = '{"param1":"a", "param2" : "b"}'
data = urllib.urlencode(data)
request = urllib2.Request('http://test.com', data, xxxx)
response = urllib2.urlopen(request)

