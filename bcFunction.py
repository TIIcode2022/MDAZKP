import hashlib
from math import log2

def hashType(data, dataType='hex'):
    """

    :param data:字符串数据
    :param dataType: 数据类型（原始数据：raw，八进制数据：hex）
    :return:(str) hash 64
    """
    if dataType == 'hex':
        return str(hashlib.sha256(bytes.fromhex(data.zfill(128))).hexdigest())
    else:
        return str(hashlib.sha256(data.encode()).hexdigest())


# Merkel树根生成
def createMerkleRoot(data, dataType='hex'):
    """

    :param data: Merkle树打包的数据（list，默认都转换为字符串数据传入）
    :param dataType: 数据类型（原始数据：raw，八进制数据：hex）
    :return:mTree [0,1,2,3,4,5,6]
              0
            1   2
           3 4 5 6
    """

    dataLen = len(data)
    highLevel=round(log2(dataLen))
    mTree = [None for i in range(2*dataLen-1)]
    mTree[-dataLen:]=[hashType(x) for x in data]


    for i in range(int(len(mTree)/2)-1,-1,-1):
        mTree[i]=hashType(mTree[2*i+1]+mTree[2*i+2])
    return mTree


#返回兄弟节点编号
def returnBrotherNodes(mTree,index):
    """
    :param mTree: 函数createMerkleRoot的返回数据类型
    :param index: 需要寻找兄弟节点的下边
    :return: [('兄弟节点hash值'，左：1右：0)]
    """
    trueIndex=index+int(len(mTree)/2)
    nowindex=trueIndex
    brotherNodes=[]
    while nowindex!=0:
        if nowindex%2==0:
            brotherNodes.append((mTree[nowindex-1],1))
            nowindex=int((nowindex-2)/2)
        else:
            brotherNodes.append((mTree[nowindex+1],0))
            nowindex=int((nowindex-1)/2)

    return brotherNodes


#区块增加块
def addBlock(data,dataType):
    mTree=createMerkleRoot(data,dataType)

    return mTree


    # nowBlock=preBlock+1

    # with open('./Merkle/'+str(nowBlock)+'.m','w') as f:
    #     f.write(str(mTree))
    # with open('./blocks/'+str(preBlock)+'.block','r') as f:
    #     content=f.read()
    #     hashV=hashlib.sha256(content.encode('utf-8')).hexdigest()
    # nowContent=str([hashV,data])
    # with open('./blocks/'+str(nowBlock)+'.block','w') as f:
    #     f.write(nowContent)



