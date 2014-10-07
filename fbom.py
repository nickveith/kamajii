# -*- coding: utf-8 -*-

import csv
import psycopg2
import psycopg2.extras
import json
import random
import os
import urllib2
import re
import time

from os.path import isfile, join
from io import BytesIO
from PIL import Image
from datetime import datetime
from datetime import timedelta
from sadlab_s3 import SadLabS3
from short import URLShorten

def fileToS3(local_path,s3_path,s3_bucket):
    s3 = SadLabS3(s3_bucket)
    response = s3.save_file(local_path,destination=s3_path)
    return response

############################################################################################################################################

class EntitiesToPromote(object):
    def __init__(self, entities_to_promote):
        self.entities_to_promote = entities_to_promote
        pass

    def get(self):
        with open(self.entities_to_promote,'rb') as f:
            records = csv.DictReader(f, delimiter='\t', quotechar = '"')
            entities = [] + [rec for rec in records]
            return entities

    def filter(self, **kwargs):
        entities = self.get()
        filtered = [n for n in entities if all(n.get(k) == v for k, v in kwargs.iteritems())]
        contentTypes = set([f['type'] for f in filtered])
        return {'contentTypes':contentTypes, 'entities':filtered}

############################################################################################################################################

class EntityToPromote(object):
    def __init__(self, content_type, identifier, PO= None, Uber= None, discount= None, destination_url= None, destination_url2= None, saleName= None, imageMask= None, copyTypes=None, copy_src=None, image_src=None):
        self.type = content_type
        self.id = identifier
        self.PO = PO
        self.Uber = Uber
        self.discount = discount
        self.destination_url = destination_url
        self.destination_url2 = destination_url2
        self.saleName = saleName
        self.imageMask = imageMask
        self.copyTypes = copyTypes
        self.copy_src = copy_src
        self.image_src = image_src
        pass

    def get(self):
        entity = None
        if self.type == 'paid class':
            if not self.copy_src:
                self.copy_src = 'courseCopy.csv'
            if not self.image_src:
                self.image_src = 'courseImage.csv'
            self.targeting = 'NONE'
            entity = PaidClass(identifier=self.id, discount=self.discount, destination_url=self.destination_url, destination_url2=self.destination_url2, saleName=self.saleName, imageMask=self.imageMask, copy_src=self.copy_src, image_src= self.image_src)
        if self.type == 'course gallery':
            if not self.copy_src:
                self.copy_src = 'courseCopy.csv'
            if not self.image_src:
                self.image_src = 'courseImage.csv'
            self.targeting = 'NONE'
            entity = CourseGallery(identifier=self.id, discount=self.discount, destination_url=self.destination_url, destination_url2=self.destination_url2, saleName=self.saleName, imageMask=self.imageMask, copy_src=self.copy_src, image_src= self.image_src)
        if self.type == 'ecomm product':
            entity = EcommProduct(self.id, self.discount, self.destination_url, self.destination_url2, saleName=self.saleName, imageMask=self.imageMask, copy_src='')
            self.targeting = 'ECOMM'
        if self.type == 'pattern':
            entity = Pattern(self.id, self.discount, self.destination_url, self.destination_url2, saleName=self.saleName, imageMask=self.imageMask, copy_src='')
            self.targeting = 'NONE'
        if self.type == 'ecomm gallery':
            entity = EcommGallery(self.id, self.discount, self.destination_url, self.destination_url2, saleName=self.saleName, imageMask=self.imageMask, copy_src='')
            self.targeting = 'ECOMM'
        if self.type == 'ecomm kit':
            entity = EcommProduct(self.id, self.discount, self.destination_url, self.destination_url2, saleName=self.saleName, imageMask=self.imageMask, copy_src='')
            self.targeting = 'ECOMM'
        return entity

    def copy(self, copyTypes=None):
        copy = self.get().copy(copyTypes)
        return copy

    def images(self):
        images = self.get().images()
        return images

    def posts(self, limit=1, copyTypes=None):
        posts = []

        copy = self.copy(copyTypes)
        images = self.images()

        for x in range(0,limit):
            c = random.choice(copy)
            i = random.choice(images)
            ext_desc = 'Content:{0}|ID:{1}|Category:{2}|Copy Type:{3}|Image Type:{4}'.format(self.type, self.id, c['category'], c['copy_type'], i['image_type'])
            post = {'copy': c, 'image': i, 'destination_url': self.destination_url, 'destination_url2': self.destination_url2, 'ext_desc': ext_desc}
            posts.append(post)

        return posts

############################################################################################################################################

class PaidClass(EntityToPromote):
    def __init__(self, identifier, discount, destination_url, destination_url2,copy_src='courseCopy.csv', image_src='courseImage.csv', imageMask= None, saleName=None, imagePath=None):
        self.id = identifier
        self.course_id = identifier
        self.copy_src = copy_src
        self.image_src = image_src
        if destination_url:
            self.destination_url = destination_url
        else:
            self.destination_url = 'http://www.craftsy.com/class/a/{course_id}'.format(course_id= self.id)
        self.destination_url2 = destination_url2
        self.discount = discount
        self.image_mask = imageMask
        self.saleName = saleName
        self.type = 'course'
        if not imagePath:
            self.image_path = '/Volumes/SHARES/Marketing/Social Sale Imagery/horizontal/'
        else:
            self.image_path = imagePath
        pass

    def _courseTemplate(self, course_name, course_category, course_instructor, course_copy, course_copy_type, discount):  
        posts = []
        ext1 = '{ext1}'
        postShells = [r'{leadText} {cta} -->> {ext1} \n\n{copy}']
        leadIns = [r'Save {pcntSavings} today on "{courseTitle}", an online class with {courseInstructor}!'
                  ,r'Get {pcntSavings} off today on "{courseTitle}", an online class with {courseInstructor}!'
                  ,r'Save big! Get {pcntSavings} off today on "{courseTitle}", an online class with {courseInstructor}!'
                  ,r'Refine your skills! Get {pcntSavings} off today on "{courseTitle}", an online class with {courseInstructor}!'
                  ,r'We love {courseTitle}, an online class with {courseInstructor}! Enroll today for {pcntSavings} off & fall in love with learning something new!'
                  ,r'Our {categoryContent} community has spoken—They love {courseTitle}, an online class with {courseInstructor}! Join them today for {pcntSavings} off!'
                  ,r"Tired of forgetting techniques from in-person workshops? Enroll in {courseTitle}, an online class with {courseInstructor}, and revisit techniques as often as you'd like with unlimited lifetime access!"
                  ,r'Tired of feeling lost in the crowd at in-person workshops? Get personalized support from your instructor in the online class {courseTitle} with {courseInstructor}, from the comfort of your home!'
                  ,r'Get better at what you love in the online class {courseTitle} with {courseInstructor}! Sign up today and save {pcntSavings}!'
                  ,r'Excited to learn something new? Join {courseTitle}, an online class with {courseInstructor}, and start learning on your own schedule, in your own home.'
                  ,r'Get inspired with {courseTitle}, an online class with {courseInstructor}! Save {pcntSavings} when you enroll today, then watch your class anytime!'
                  ,r'Build your skills in {courseTitle}, an online class with {courseInstructor}! Save {pcntSavings} & get a 100% money back guarantee.'
                  ,r'Have fun learning with {courseInstructor} in the online class {courseTitle}! Get {pcntSavings} off & unlimited lifetime access today!'
                  ,r'Indulge your creative side with {courseTitle}, an online class with {courseInstructor}! Get {pcntSavings} off when you enroll today, then watch your class anytime with unlimited lifetime access!'
                  ,r'Explore your passion with instructor {courseInstructor} in the online class {courseTitle}! Save {pcntSavings} when you enroll today.'
                  ,r'Learn more about what you love in {courseTitle}, an online class with {courseInstructor}! Get {pcntSavings} off when you enroll today, then enjoy learning in your own home and on your own schedule!'
                  ,r'Passionate members of our {categoryContent} community love what they have learned in {courseTitle}, an online class with {courseInstructor}! Join the fun today for {pcntSavings} off!'
                  ]
        CTAs = ['Click'
                 ,'Learn More'
                 ,'Get Started'
                 ,'Enroll'
                 ,'Start Now'
                 ,'Start learning'
                 ,'Get inspired'
                 ]

        for s in postShells:
            for l in leadIns:
                for c in CTAs:
                    postCopy = s.format(leadText=l, ext1=ext1, copy=course_copy, cta=c).format(courseTitle=course_name, cta=c, courseInstructor=course_instructor, categoryContent=course_category, pcntSavings=discount, ext1=ext1)
                    posts.append({'copy':postCopy,'copy_type':course_copy_type,'category': course_category})
        return posts

    def copy(self,copyTypes=None):
        results = []
        with open(self.copy_src,'rb') as f:
            records = csv.DictReader(f, delimiter='\t', quotechar = '"')
            for rec in records:
                course_name = rec['name']
                instructor = rec['instructor']
                category = rec['category']
                if rec["id"] == self.id:
                    if copyTypes is None:
                        copyTypes = [x for x in rec.keys() if x not in ['id','name','category','instructor','page']]
                    for copyType in copyTypes:
                        copy = rec[copyType]    
                        if copy != '':                    
                            posts = self._courseTemplate(course_name=course_name, course_category=category, course_instructor=instructor, course_copy=copy, course_copy_type=copyType, discount=self.discount)
                            results = results + [post for post in posts]
        # return 'course copy list', self.course_id
        return results

    def images(self):
        if not self.image_mask:
            imageTypes = ['hero','inst','tech']  
            images = []

            with open(self.image_src,'rb') as f:
                records = csv.DictReader(f, delimiter=',', quotechar = '"')
                images = images + [rec for rec in records if rec["course_id"] == self.id]

            if images == []:
                images = [{'image_url':'http://static-sympoz.s3.amazonaws.com/course/{course_id}/titleCard.jpg'.format(course_id = self.id)
                        ,'image_type':'title'
                        ,'size':'693x393'
                        ,'course_id': self.id
                        ,'file_format':'jpg'}]

            return images
        else:
            projectDir = os.path.dirname(os.path.realpath(__file__))
            image_path = self.image_path
            image_files = [f for f in os.listdir(image_path) if isfile(join(image_path,f)) and re.match(self.id + '_',f) is not None and re.match('._',f) is None]
            image_format = 'JPEG'

            images = []

            for b in image_files:
                image = {}
                image_name = b.split('.')[0] + '_' + self.saleName.replace(' ','_') + '.' + image_format
                local_path = projectDir + '/' + image_name
                s3_path = 'promo_images/' + self.saleName + '/' + image_name
                background = Image.open(join(image_path,b))
                
                if self.image_mask:
                    foreground = Image.open(BytesIO(urllib2.urlopen(self.image_mask).read()))
                    background.paste(foreground, (0,0), foreground)
                
                background.save(join(projectDir,image_name),image_format)
                s3 = fileToS3(local_path,s3_path,'sympoz-social-assets')
                image_url = 'https://sympoz-social-assets.s3.amazonaws.com/' + urllib2.quote(s3_path)
                os.remove(local_path)
                
                image['image_url'] = image_url
                image['image_type'] = 'horizontal'
                image['size'] = '1200x1200'
                image['file_format'] =  'JPG'
                image['id'] =  self.id

                images.append(image)

            return images


############################################################################################################################################

class CourseGallery(EntityToPromote):
    def __init__(self, identifier, discount=None, destination_url=None, destination_url2=None,copy_src='copy_src.csv', image_src='image_src.csv', imageMask= None, saleName=None):
        self.id = identifier
        self.course_gallery_id = identifier
        self.copy_src = copy_src
        self.image_src = image_src
        self.destination_url = destination_url
        self.destination_url2 = destination_url2
        self.discount = discount
        self.image_mask = imageMask
        self.saleName = saleName
        pass

    def copy(self,copyTypes):
        results = []
        ext1 = '{ext1}'
        with open(self.copy_src,'rb') as f:
            records = csv.DictReader(f, delimiter='\t', quotechar = '"')
            for rec in records:
                name = rec['name']
                category = rec['category']
                if rec["id"] == self.id:
                    if not copyTypes:
                        copyTypes = [x for x in rec.keys() if x not in ['id','name','category','instructor','page']]
                    for copy_type in copyTypes:
                        copy = rec.get(copy_type,'')
                        if not copy:
                            copy = ''
                        if copy != '':    
                            copy_complete = copy.format(galleryName=name, category=category, pcntSavings=self.discount, saleName=self.saleName, ext1=ext1)                
                            posts = [{'copy':copy_complete, 'copy_type':copy_type,'category': category}]
                            results = results + [post for post in posts]
        # return 'course copy list', self.course_id
        return results

    def images(self):
        projectDir = os.path.dirname(os.path.realpath(__file__))
        image_path = '/Volumes/SHARES/Marketing/Social Sale Imagery/collage/'
        image_files = [f for f in os.listdir(image_path) if isfile(join(image_path,f)) and re.match(self.id + '_',f) is not None and re.match('._',f) is None]
        image_format = 'JPEG'

        images = []

        for b in image_files:
            image = {}
            image_name = b.split('.')[0] + '_' + self.saleName.replace(' ','_') + '.' + image_format
            local_path = projectDir + '/' + image_name
            s3_path = 'promo_images/' + self.saleName + '/' + image_name
            background = Image.open(join(image_path,b))
            
            if self.image_mask:
                foreground = Image.open(BytesIO(urllib2.urlopen(self.image_mask).read()))
                background.paste(foreground, (0,0), foreground)
            
            background.save(join(projectDir,image_name),image_format)
            s3 = fileToS3(local_path,s3_path,'sympoz-social-assets')
            image_url = 'https://sympoz-social-assets.s3.amazonaws.com/' + urllib2.quote(s3_path)
            os.remove(local_path)
            
            image['image_url'] = image_url
            image['image_type'] = 'collage'
            image['size'] = '1200x1200'
            image['file_format'] =  'JPG'
            image['id'] =  self.id

            images.append(image)

        return images

############################################################################################################################################

class EcommGallery(EntityToPromote):
    def __init__(self, copy_src='ecommProductCopy.csv', image_src='ecommProductImage.csv', imageMask= None, saleName=None):
        self.id = identifier
        self.ecomm_gallery_id = identifier
        self.copy_src = copy_src
        self.image_src = image_src
        self.destination_url = destination_url
        self.destination_url2 = destination_url2
        self.discount = discount
        self.imageMask = imageMask
        self.saleName = saleName
        pass

    def copy(self,copyTypes):
        results = []
        ext1 = '{ext1}'
        ext2 = '{ext2}'
        with open(self.copy_src,'rb') as f:
            records = csv.DictReader(f, delimiter='\t', quotechar = '"')
            for rec in records:
                name = rec['name']
                category = rec['category']
                if rec["id"] == self.id:
                    if not copyTypes:
                        copyTypes = [x for x in rec.keys() if x not in ['id','name','category']]
                    for copy_type in copyTypes:
                        copy = rec[copy_type]    
                        if copy != '':    
                            copy_complete = copy.format(galleryName=name, category=category, pcntSavings=self.discount, saleName=self.saleName, ext1=ext1, ext2=ext2)                
                            posts = [{'copy':copy_complete, 'copy_type':copy_type,'category': category}]
                            results = results + [post for post in posts]
        # return 'course copy list', self.course_id
        return results

    def images(self):
        images = []

        with open(self.image_src,'rb') as f:
            records = csv.DictReader(f, delimiter='\t', quotechar = '"')
            images = images + [rec for rec in records if rec["id"] == self.id]

        if self.imageMask:
            for b in images:
                background = Image.open(BytesIO(urllib2.urlopen(b['image_url']).read()))
                foreground = Image.open(BytesIO(urllib2.urlopen(self.imageMask).read()))
                background.paste(foreground, (0,0), foreground)
                # background.show()
                
                projectDir = os.path.dirname(os.path.realpath(__file__))
                image_format = 'JPEG'
                image_name = self.id + '_' + self.saleName.replace(' ','_') + '.' + image_format
                local_path = projectDir + '/' + image_name
                s3_path = 'promo_images/Big Fall Sale/' + image_name

                background.save(projectDir + '/' + image_name,image_format)
                s3 = fileToS3(local_path,s3_path,'sympoz-social-assets')
                image_url = 'https://sympoz-social-assets.s3.amazonaws.com/' + urllib2.quote(s3_path)
                os.remove(local_path)
                b['image_url'] = image_url

        return images

############################################################################################################################################

class EcommProduct(EntityToPromote):
    def __init__(self, identifier, discount, destination_url, destination_url2,copy_src='ecommProductCopy.csv', image_src='ecommProductImage.csv', imageMask= None, saleName=None):
        self.id = identifier
        self.pfs_id = identifier
        self.copy_src = copy_src
        self.image_src = image_src
        self.destination_url = destination_url
        self.destination_url2 = destination_url2
        self.discount = discount
        self.imageMask = imageMask
        self.saleName = saleName
        pass

    def copy(self,copyTypes):
        results = []
        with open(self.copy_src,'rb') as f:
            records = csv.DictReader(f, delimiter='\t', quotechar = '"')
            for rec in records:
                if not copyTypes:
                    copyTypes = [x for x in rec.keys() if x not in ['id','name','category']]
                if rec["id"] == self.pfs_id:
                    for copyType in copyTypes:
                        copy = rec[copyType]
                        if copy != '':
                            copy = [{'copy_type':copyType,'copy':copy,'category': rec['category']}]
                            results = results + [c for c in copy]                        
        return results

    def images(self):
        images = []
        with open(self.image_src,'rb') as f:
            records = csv.DictReader(f, delimiter='\t', quotechar = '"')
            for rec in records:
                if rec['id'] == self.pfs_id:
                    image = {'image_url': rec['image_url']
                            ,'image_type': rec['image_type']
                            ,'size': rec['size']
                            ,'course_id': self.id
                            ,'file_format': rec['file_format']}
                    images.append(image)
        return images

############################################################################################################################################

class Pattern(EntityToPromote):
    def __init__(self, identifier, discount, destination_url, destination_url2,copy_src, image_src, imageMask= None, saleName=None):
        self.id = identifier
        self.pattern_id = identifier
        self.copy_src = copy_src
        self.image_src = image_src
        self.destination_url = destination_url
        self.destination_url2 = destination_url2
        self.discount = discount
        self.imageMask = imageMask
        self.saleName = saleName
        pass

    def copy(self,copyTypes):
        results = []
        if not copyTypes:
            copyTypes = [x for x in rec.keys() if x not in ['id','name','category']]
        with open(self.copy_src,'rU') as f:
            records = csv.DictReader(f, delimiter='\t', quotechar = '"')
            for rec in records:                    
                if rec["pattern_id"] == self.pattern_id:
                    copy = {'copyType':'Long Form','copy':rec['final_copy']}
                    results.append(copy)
        return results

    def images(self):  
        results = []
        with open(self.image_src,'rU') as f:
            records = csv.DictReader(f, delimiter='\t', quotechar = '"')
            copyType = 'Long Form'
            for rec in records:                    
                if rec["pattern_id"] == self.pattern_id:
                    images = [rec['full_image_url']]
                    results = results + [i for i in images]
        return results

############################################################################################################################################

class Pages(object):
    def __init__(self, page_src= 'pageStatus.csv'):
        self.page_src = page_src
        pass
    
    def get(self):
        with open(self.page_src,'rb') as f:
            records = csv.DictReader(f, delimiter='\t', quotechar = '"')
            results = [] + [rec for rec in records if (True)]
            return results

############################################################################################################################################

class Page(object):
    def __init__(self, page_id, page_name, shortname):
        self.page_id = page_id
        self.page_name = page_name
        self.page_shortname = shortname
    # def __init__(self, **kwargs):
    #     for k, v in kwargs.iteritems():
    #         k = v
    #     pass

############################################################################################################################################

class Schedule(object):
    def __init__(self, start_datetime, end_datetime, schedule_src= 'pageSchedule.csv'):
        self.schedule_src = schedule_src
        self.start_datetime = datetime.strptime(start_datetime,'%Y%m%d')
        self.end_datetime = datetime.strptime(end_datetime,'%Y%m%d')
        pass
    
    def get(self):
    # import schedule
        d = self.start_datetime
        delta = timedelta(days=1)
        results = []
        while d < self.end_datetime:
            with open(self.schedule_src,'rU',) as f:
                records = csv.DictReader(f, delimiter='\t', quotechar = '"')
                for rec in records:
                    if rec["dayOfWeek"] == d.strftime('%A'):
                        rec["daydate"] = d
                        rec["datetime"] = datetime.strptime(datetime.strftime(d,'%Y%m%d') + rec["Time"],'%Y%m%d%H:%M:%S')
                        rec['fb_time_str'] = datetime.strftime(rec["daydate"],'%Y-%m-%d') + 'T' + datetime.strftime(rec["datetime"],'%H:%M:%S')
                        rec['ext_time_str'] = datetime.strftime(rec["daydate"],'%Y%m%d') + "_" + datetime.strftime(rec["datetime"],'%H%M')
                        results.append(rec)
            d += delta
        return results

    def filter(self, **kwargs):
        schedule = self.get()
        return [n for n in schedule if all(n.get(k) == v for k, v in kwargs.iteritems())]

class ExtBulkSheet(object):
    def __init__(self, marketing_action_name):
        self.rows = []
        self.headers = []
        self.marketing_action_name = marketing_action_name
        pass

    def add(self, ext_key, destination_url, ext_desc, PO=None, Uber=None, reg_prompt='FALSE', reg_type=None, skippable=None):
        record = {}

        if PO is None and Uber is None:
            Uber = 'dummy_uber'

        if reg_prompt != 'FALSE' and (reg_type is None or skippable is None):
            print 'You have to specify reg type and skippable T/F if you are going to use reg prompt'
            raise

        record['Description']= ext_desc
        record['Promotional Offer Code']= PO
        record['Uber Offer Code']= Uber
        record['Landing URL']= destination_url
        record['ext link key']= ext_key
        record['active']= 'TRUE'
        record['Campaign']= None
        record['Marketing action']= self.marketing_action_name
        record['Vanity']= 'FALSE'
        record['Affiliate']= '1'
        record['Registration Prompt (true/false)']= reg_prompt
        record['Registration Type']= reg_type
        record['Skippable (true/false)']=skippable
        record['Image URL']=None
        record['Message']=None
        record['Button Text']=None
        record['Pricing Expiration Date']=None

        self.rows.append(record)

    def write(self, file_to_write_to= 'bulk_ext_out.csv'):
        rowsToWrite = self.rows
        file_to_write_headers = ['Description','Promotional Offer Code','Uber Offer Code','Landing URL','ext link key','active','Campaign','Marketing action','Vanity','Affiliate','Registration Prompt (true/false)','Registration Type','Skippable (true/false)','Image URL','Message','Button Text','Pricing Expiration Date']
        
        with open(file_to_write_to,'wb') as f:
            writer = csv.writer(f, dialect ='excel')
            writer.writerow(file_to_write_headers)
            dictwriter = csv.DictWriter(f, file_to_write_headers, dialect ='excel')
            dictwriter.writerows(rowsToWrite)
            print 'GREAT SUCCESS! Wrote some rows to this file --> {0}'.format(file_to_write_to)

############################################################################################################

# identifier = '4726'
# paidClass = EntityToPromote(content_type='paid class', identifier=identifier, imageMask= None, saleName= None)
# print paidClass.images()





