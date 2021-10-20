import requests
import schedule
import time
from neo4j import GraphDatabase

#Initiate Neo4j Driver
class neoApp:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

def response(uri):
    resp = requests.get('http://localhost:8080/'+uri,
    auth=('apikey','6074b1b8f0ed3091b9d170a989d54231338ed4a4e716ed0f8b65550ffd0c283a'))

    return resp

projecturi = 'api/v3/projects'
wpuri = 'api/v3/work_packages?query_props=%7B"c"%3A%5B"id"%2C"subject"%2C"type"%2C"status"%2C"assignee"%2C"priority"\
    %5D%2C"tv"%3Afalse%2C"hl"%3A"none"%2C"hi"%3Atrue%2C"g"%3A""%2C"t"%3A"id%3Aasc"%2C"f"%3A%5B%7B"n"%3A"status"\
    %2C"o"%3A"*"%2C"v"%3A%5B%5D%7D%5D%2C"pa"%3A1%2C"pp"%3A20%7D'
useruri = 'api/v3/users'
neo = neoApp('bolt://localhost:7687','neo4j','12345')

#Running Neo4j Query
def neoRun(query,param = None):
    with neo.driver.session() as session:
        session.run(query,param)
        session.close

def clear():
    neoRun('match (a) -[r] -> () delete a, r')
    neoRun('match (a) delete a')


# Create Project Query
def cre_pro():
    create_project ='''
    merge (p:Project {id: $id})
    on create set p.name = $name,
    p.active = $active,
    p.description = $description,
    p.statusdesc = $statusExplanation,
    p.createdAt = $createdAt,
    p.updatedAt = $updatedAt,
    p.status = $status
    '''
    for i in response(projecturi).json()['_embedded']['elements']:
        id = i['id']
        name = i['name']
        active = i['active']
        desc = i['description']['raw']
        creTime = i['createdAt']
        upTime = i['updatedAt']
        staDesc = i['statusExplanation']['raw']
        stats = 'null' if i['_links']['status']['href'] is None else i['_links']['status']['href']
        neoRun(create_project,param={'name':name,'id':id,'active':active,'description':desc,'statusExplanation':staDesc,\
            'createdAt':creTime,'updatedAt':upTime,'status':stats})

def pro_rel():
    relationship_project ='''
    match (n:Project {id:$paID}),(m:Project {id:$chiID})
    merge (n)-[:ChildProject]-> (m)
    '''
    for i in response(projecturi).json()['_embedded']['elements']:
        parent = i['_links']['parent'] 
        id = i['id']
        if parent['href'] is not None:
            paId = parent['href'].rsplit('/',1)[::-1]
            neoRun(relationship_project,param={'paID':int(paId[0]),'chiID':id})
            
def cre_wp():
    Create_wp = '''
    merge (wp:WorkPackage {id:$id})
    on create set wp.name = $name,
    wp.description = $desc,
    wp.type = $type,
    wp.priority = $priority,
    wp.status = $status,
    wp.percentageDone = $perc,
    wp.createdAt = $creTime,
    wp.updatedAt = $upTime
    '''

    for i in response(wpuri).json()['_embedded']['elements']:
        link = i['_links']
        id = i['id']
        name = i['subject']
        desc = i['description']['raw']
        type = link['type']['title']
        priority = link['priority']['title']
        stats = link['status']['title']
        perdone = i['percentageDone']
        cretime = i['createdAt']
        uptime = i['updatedAt']
        neoRun(Create_wp, param={'id':id,'desc':desc,'name':name,'type':type,'priority':priority,'status':stats,\
            'perc':perdone,'creTime':cretime,'upTime':uptime})
        if link['project']['href'] is not None and link['parent']['href'] is None:
            proid = link['project']['href'].rsplit('/',1)[::-1]
            wp_pro(int(proid[0]),id)
        if link['parent']['href'] is not None:
            chid = link['parent']['href'].rsplit('/',1)[::-1]
            wp_child(int(chid[0]),id)
        if link['author']['href'] is not None:
            userid = link['author']['href'].rsplit('/',1)[::-1]
            wp_author(id,int(userid[0]))
        if link['assignee']['href'] is not None:
            userid = link['assignee']['href'].rsplit('/',1)[::-1]
            wp_assign(id,int(userid[0]))
        if link['responsible']['href'] is not None:
            userid = link['responsible']['href'].rsplit('/',1)[::-1]
            wp_resp(id,int(userid[0]))
        if response(link['watchers']['href']).json()['count']>0:
            for u in response(link['watchers']['href']).json()['_embedded']['elements']:
                wp_watcher(id,u['id'])
        a = response(link['activities']['href']).json()
        if a['count']>0:
            previd = 0
            for e in a['_embedded']['elements']:
                actid = e['id']
                date = e['createdAt']
                for d in e['details']:
                    if 'Progress' in d['raw']:
                        det = d['raw']
                        if previd == 0:
                            cre_prog(id,actid,date,det)
                            previd = actid
                        else:
                            cre_prog(id,actid,date,det,previd)
                            previd = actid
        
def cre_user():
    Create_users = '''
    merge (u:User {id:$id})
    on create set u.name = $name,
    u.email = $email
    '''

    for i in response(useruri).json()['_embedded']['elements']:
        id = i['id']
        name = i['name']
        email = i['email']
        neoRun(Create_users,param={'id':id,'name':name,'email':email})

def wp_pro(proid,wpid):
    query = '''
    match (p:Project{id:$proid}),(w:WorkPackage{id:$wpid})
    merge (p)-[:WorkPackage]->(w)
    '''
    neoRun(query,param={'proid':proid,'wpid':wpid})

def wp_child(wpid,chid):
    query = '''
    match (w:WorkPackage{id:$wpid}),(n:WorkPackage{id:$chid})
    merge (w)-[:Child]->(n)
    '''
    neoRun(query,param={'wpid':wpid,'chid':chid})

def wp_author(wpid,userid):
    query = '''
    match (w:WorkPackage{id:$wpid}),(u:User{id:$userid})
    merge (w)-[:Author]->(u)
    '''
    neoRun(query,param={'wpid':wpid,'userid':userid})

def wp_assign(wpid,userid):
    query = '''
    match (w:WorkPackage{id:$wpid}),(u:User{id:$userid})
    merge (w)-[:Assignee]->(u)
    '''
    neoRun(query,param={'wpid':wpid,'userid':userid})

def wp_resp(wpid,userid):
    query = '''
    match (w:WorkPackage{id:$wpid}),(u:User{id:$userid})
    merge (w)-[:Accountable]->(u)
    '''
    neoRun(query,param={'wpid':wpid,'userid':userid})

def wp_watcher(wpid,userid):
    query = '''
    match (w:WorkPackage{id:$wpid}),(u:User{id:$userid})
    merge (w)-[:Watcher]->(u)
    '''
    neoRun(query,param={'wpid':wpid,'userid':userid})

def cre_prog(wpid,progid,cretime,det,previd=0):
    fquery = '''
    match (w:WorkPackage {id:$wpid})
    merge (w)-[:ProgressChange]->(p:Progress{id:$id})
    on create set p.createdAt = $cretime,
    p.details = $det
    '''
    query = '''
    match (w:Progress {id:$progid})
    merge (w)-[:ProgressChange]->(p:Progress{id:$id})
    on create set p.createdAt = $cretime,
    p.details = $det
    '''
    if previd == 0:
        neoRun(fquery,param={'wpid':wpid,'id':progid,'cretime':cretime,'det':det})
    else:
        neoRun(query,param={'progid':previd,'id':progid,'cretime':cretime,'det':det})

def pyrun():
    clear()
    cre_pro()
    pro_rel()
    cre_user()
    cre_wp()

schedule.every(30).seconds.do(pyrun)
  
while True:
    schedule.run_pending()
    time.sleep(1)
