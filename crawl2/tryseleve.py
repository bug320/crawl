# -*- coding:utf-8 -*-
import shelve


if __name__ == "__main__":

    # #show the page url
    # sdb = shelve.open("page.she","r")
    # i = 0
    # for k,v in sdb.iteritems():
    #     print k,v
    #     i+=1
    #     pass
    # print i
    # sdb.close()

    #show the subpage url
    sdb = shelve.open("subpage.she", "r")
    i = 0
    for k, v in sdb.iteritems():
        print k, v
        i += 1
        if i%10 ==0 :
            print ""
        pass
    print i
    sdb.close()
    pass