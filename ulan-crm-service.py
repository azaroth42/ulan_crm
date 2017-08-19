
from bottle import Bottle, route, run, request, response, abort, error, redirect
  
from rdflib import ConjunctiveGraph, URIRef
from pyld import jsonld
from pyld.jsonld import compact, expand, frame, from_rdf, to_rdf, JsonLdProcessor
import json
import codecs
import sys
import requests
import os

from cromulent.model import factory, Person, Type, InformationObject, \
	Appellation, Group, TimeSpan, Place, BeginningOfExistence, EndOfExistence, \
	Actor, Creation, Activity 
from cromulent.vocab import WebPage, Nationality, Gender, BiographyStatement, \
	PrimaryName, Description, Active
from cromulent.extra import add_rdf_value, add_schema_properties
add_rdf_value()
add_schema_properties()

factory.base_url = "http://vocab.getty.edu/ulan/"
factory.base_dir = "data"
baseUrl = "http://vocab.getty.edu/"

# Base ULAN context and frame
fh = file('/home/azaroth/web_services/context.json')
ctxt = fh.read()
fh.close()
context_js = json.loads(ctxt)

frame_js = {"@context": context_js['@context'],
			"type": "skos:Concept",
			"contributor": {"@embed": False},
			"source": {"@embed": False},
			"changeNote": {"@embed": False},
			"note": {"@embed": False},

			"mappingRelation": {"@embed": False},
			"exactMatch": {"@embed": False},
			"closeMatch": {"@embed": False}

			# "conceptFor": {
			# 	"biography": {"@embed": False},
			# 	"biographyNonPref": {"@embed": False},
			# 	"biographyPref": {"@embed": False},
			# 	"event": {"@embed": False},
			# 	"eventPref": {"@embed": False},
			# 	"eventNonPref": {"@embed": False}
			# }
		}

class ULAN_CRM_Server(object):

	def __init__(self):
		self.cache = {}
		self.json_cache = {}
		self.DO_SOURCES = False		
		self.prop_data = {
			"BeginningOfExistence": [
				["timespan", "begin_of_the_begin"],
				["timespan", "end_of_the_end"],
				["took_place_at", "id"]
			],
			"EndOfExistence": [
				["timespan", "begin_of_the_begin"],
				["timespan", "end_of_the_end"],
				["took_place_at", "id"]
			],
			"Type": [
				["id"]
			],
			"Group": [
				["classified_as", "label"]
			]
		}

	def expand_url(self, url):
		url = url.replace('aat:', context_js['@context']['aat'])
		url = url.replace('ulan:', context_js['@context']['ulan'])	
		url = url.replace('tgn:', context_js['@context']['tgn'])
		return url

	def fetch_graph(self, url, do_frame=True):
		if len(url) < 5:
			raise ValueError(url)
		url = self.expand_url(url)
		if url[2:].find("http://") > -1:
			raise ValueError(url)
		if not url.endswith('.ttl'):
			url = url + ".ttl"

		if url in self.json_cache:
			return self.json_cache[url]
		elif url in self.cache:
			rdf = self.cache[url]
		else:
			print "Fetching %s" % url
			fh = requests.get(url)
			rdf = fh.text
			fh.close()
			self.cache[url] = rdf

		g = ConjunctiveGraph()
		try:
			g.parse(data=rdf, format="turtle")
		except:
			# just treat like it doesn't exist
			abort(404)
		out = g.serialize(format='json-ld')
		try:
			out = out.decode('utf-8')
		except:
			pass

		atjs = json.loads(out)
		if do_frame:
			j2 = {"@context": context_js, "@graph": atjs}
			atjs = frame(j2, frame_js)
		atjs = compact(atjs, context_js)
		try:
			del atjs['@context']
		except:
			pass

		self.json_cache[url] = atjs
		if len(self.json_cache) > 200:
			print "JSON CACHE now %s" % (len(json_cache))
			# Trash some out of it
		return atjs


	def clean_json(self, what):
		togo = ["changeNote", "ccLicense", "created",  "displayOrder", "identifier", \
			"generatedBy", "license", "mappingRelation", "modified", "parentStr", \
			"parentStrAbbr", "scheme", 'note']

		def clean(po):
			for p in po.keys():
				if p in togo or p.startswith('broader'):
					del po[p]
		clean(what)

		descend = ['altLabelObj', 'prefLabelObj', 'scopeNote', 'conceptFor']
		for d in descend:
			if d in what:
				if type(what[d]) == list:
					for po in what[d]:
						clean(po)
				else:
					clean(what[d])	
		# Changes are by ref, so what is modified in place, but return it anyway
		return what

	def self.data_exists(self, new, olds):
		# BegofExist:  timespan.botb, timespan.eote, took_place_at.id		
		props = self.prop_data.get(new.type, [])
		if not props:
			return False

		for o in olds:
			for p in props:
				for pi in p:
					op = getattr(o, p, None)
					np = getattr(new, p, None)
					if op and np and op != np:
						return False
		return True


	def process_bio(self, who, bp, pref=True):
		birth = bp.get('estStart', {'@value':''})['@value']
		death = bp.get('estEnd', {'@value':''})['@value']
		birthplace = bp.get('birthPlace', '')
		deathplace = bp.get('deathPlace', '')
		gender = bp.get('gender', '')
		desc = bp.get('personDescription', '')
		contrib = bp.get('contributor', '')

		bev = BeginningOfExistence()
		if birth:
			bts = TimeSpan()
			bts.begin_of_the_begin = birth
			bts.end_of_the_end = birth
			bev.timespan = bts
		if birthplace:
			bj = self.fetch_graph(birthplace.replace('-place', ''), False)
			p = Place(self.expand_url(birthplace))
			p.label = bj.get('label', bj.get('rdfs:label', bj.get('skos:prefLabel')))
			bev.took_place_at = p
		if birth or birthplace:
			if pref:
				who.brought_into_existence_by = bev
			else:
				if not self.data_exists(bev, who.brought_into_existence_by):
					who.brought_into_existence_by = bev			

		eev = EndOfExistence()
		if death:
			ets = TimeSpan()
			ets.begin_of_the_begin = death
			ets.end_of_the_end = death
			eev.timespan = ets
		if deathplace:
			bj = self.fetch_graph(deathplace.replace('-place', ''))
			p = Place(self.expand_url(deathplace))
			p.label = bj.get('label', bj.get('rdfs:label', bj.get('skos:prefLabel')))
			eev.took_place_at = p
		if death or deathplace:
			if pref:				
				who.taken_out_of_existence_by = eev
			else:
				if not self.data_exists(eev, who.taken_out_of_existence_by):
					who.taken_out_of_existence_by = eev

		if gender and gender != "aat:300400512":
			g = Gender()
			g.classified_as = Type(gender)
			gj = self.fetch_graph(gender)
			g.label = gj['label']
			if pref:
				who.member_of = g
			else:
				if not self.data_exists(g, who.member_of):
					who.member_of = g

		if desc:
			bio = BiographyStatement()
			bio.value = desc
			who.referred_to_by = bio
			if contrib:
				cre = Creation()
				bio.created_by = cre
				# XXX fetch and check Person / Group
				cre.carried_out_by = Actor(self.expand_url(contrib))

	def process_event(self, who, ep):
		uri = ep['id']
		start = ep.get('estStart', '')
		end = ep.get('estEnd', '')
		comment = ep.get('comment', '')
		where = ep.get('location', '')

		btyp = ep.get('bioType', '')

		xuri = self.expand_url(uri)
		if xuri == "http://vocab.getty.edu/aat/300393177":
			ad = Active(xuri)
			who.carried_out = ad
		else:
			ad = Activity(xuri)
			ad.classified_as = Type(xuri)
			who.present_at = ad
		if start or end:
			ts = TimeSpan()
			ts.begin_of_the_begin = start or end
			ts.end_of_the_end = end or start
			ad.timespan = ts
		if comment:
			ts.label = comment
		if where:
			loc = Place(self.expand_url(ep['location']))
			ad.took_place_at = loc


	def process_term(self, new, old):
		try:
			new.value = old['literalValue']
		except:
			print repr(old)
		if old.get('termKind', '') == "http://vocab.getty.edu/term/kind/Pseudonym":
			new.classified_as = Type("http://vocab.getty.edu/aat/300404657")
		if old.get('flag', '') == "http://vocab.getty.edu/term/flag/Vernacular":
			new.classified_as = Type("http://vocab.getty.edu/aat/__vernacular")
		if old.get('display', '') == "http://vocab.getty.edu/term/display/Indexing":
			new.classified_as = Type("http://vocab.getty.edu/aat/300404668")
		if self.DO_SOURCES:
			self.process_source(new, old)

	def process_source(self, new, old):

		# dedupe multiple properties
		srcs = {}
		for x in ['sourcePref', 'sourceNonPref', 'source']:
			if x in old:
				vals = old[x]
				if type(vals) != list:
					vals = [vals]
				for s in vals:
					if type(s) == dict:
						# part
						url = self.expand_url(s['id'])
						if url in srcs:
							continue
						part = InformationObject(url)
						part.label = s['locator']
						full = InformationObject(self.expand_url(s['partOf']))
						part.composed_from = full
						new.composed_from = part
						srcs[part.id] = 1
					else:
						# string uri to source
						url = self.expand_url(s)
						if url in srcs:
							continue
						full = InformationObject(url)
						new.composed_from = full
						srcs[full.id] = 1

					js = self.fetch_graph(full.id, False)
					# Look through @graph list for full.id
					if '@graph' in js:
						for w in js['@graph']:
							if self.expand_url(w['id']) == full.id:
								# Only valuable thing is title, which is more description :(
								# And shortTitle which isn't even a title...
								try:
									full.label = w['shortTitle']
								except KeyError:
									pass
								try:
									full.description = w['title']
								except KeyError:
									pass

	def build_main(self, fn):
		main = self.fetch_graph(fn)
		self.clean_json(main)

		uri = self.expand_url(main['id'])
		# Now remodel in CRM
		if "gvp:PersonConcept" in main['type']:
			who = Person(uri)
		else:
			who = Group(uri)
		return (main, who)

	def process(self, main, who):
		# xl:prefLabel --> preferred Appellation
		pref = main['prefLabelObj']
		name = PrimaryName()
		if type(pref) != list:
			pref = [pref]
		for p in pref:
			self.process_term(name, p)
		who.identified_by = name

		# Other labels --> Appellation
		alo = main.get('altLabelObj', [])
		if type(alo) != list:
			alo = [alo]
		for o in alo:
			name = Appellation()
			self.process_term(name, o)
			who.identified_by = name

		# agentType --> Group with P2
		ats = main['agentType']
		if type(ats) != list:
			ats = [ats]
		for pref in ats:
			group = Group()
			group.classified_as = Type(pref)
			try:
				gd = self.fetch_graph(pref)
			except:
				raise
			labels = {}
			plo = gd.get('prefLabelObj', [])
			if type(plo) != list:
				plo = [plo]
			for pl in plo:
				labels[pl['literalValue']['@language']] = pl['literalValue']['@value']
			group.label = labels
			who.member_of = group

		# copy exactMatch, other than self (!!)
		xMatch = main.get('exactMatch', [])
		if type(xMatch) != list:
			xMatch = [xMatch]
		for m in xMatch:
			if m != main['id']:
				who.exact_match = Person(m)

		cMatch = main.get('closeMatch', [])
		if type(cMatch) != list:
			cMatch = [cMatch]
		if cMatch:
			cl = set(main['closeMatch']).difference(set(xMatch))
			for c in cl:
				who.close_match = Person(c)

		# scopeNote to Linguistic Object pattern to allow for source
		if 'scopeNote' in main:
			sn = main['scopeNote']
			d = Description(self.expand_url(sn['id']))
			d.value = {sn['value']['@language']: sn['value']['@value']}
			if self.DO_SOURCES:
				self.process_source(d, sn)

		# seeAlso is a webpage
		wp = WebPage(main['seeAlso'])
		who.referred_to_by = wp

		# copy void:inDataset ?

		rels = main.get('related', [])
		if type(rels) != list:
			rels = [rels]
		for k in main.keys():
			if k.startswith("gvp:ulan"):
				rel = main[k]
				if type(rel) != list:
					rel = [rel]
				for r in rel:
					ru = r['id']
					try:
						rels.remove(ru)		
					except:
						pass
					print "%s: %s" % (k, ru)
					#if not ru in self.done and not ru in fn:
					#	fn.append(ru)
		for r in rels:
			# XXX Could be a group :S  fetch and check
			who.related = Person(r)
		actor = main['conceptFor']

		# event, eventPref, eventNonPref
		ep = actor.get('eventPref', {})
		if ep:
			self.process_event(who, ep)

		eps = actor.get('eventNonPref', [])
		if type(eps) != list:
			eps = [eps]
		for ep in eps:
			self.process_event(who, ep)

		# nationality, nationalityPref, nationalityNonPref

		np = actor.get('nationalityPref', {})
		n = Nationality()
		nj = self.fetch_graph(np)
		n.label = nj['label']
		n.classified_as = Type(self.expand_url(np))
		who.member_of = n

		nnp = actor.get('nationalityNonPref', [])
		if type(nnp) != list:
			nnp = [nnp]
		# Remove "undetermined" as pointless
		try:
			nnp.remove('aat:300379012')
		except:
			pass
		for np in nnp:
			n = Nationality()
			nj = self.fetch_graph(np)
			n.label = nj['label']
			n.classified_as = Type(self.expand_url(np))
			who.member_of = n	

		# biography, biographyPref, biographyNonPref
		# estStart, estEnd
		# birthPlace, deathPlace
		# gender
		# personDescription

		bp = actor['biographyPref']
		self.process_bio(who, bp)

		nbp = actor.get('biographyNonPref', [])
		if type(nbp) != list:
			nbp = [nbp]
		for bp in nbp:
			self.process_bio(who, bp, False)

	def handle_id(self, ulan):
		if not ulan.isdigit():
			abort(404)

		ulan = "http://vocab.getty.edu/ulan/%s" % ulan

		(main, who) = self.build_main(ulan)
		self.process(main, who)
		ulfn = who.id.replace("ulan:", "")
		#factory.toFile(who, compact=False, filename="data/%s.json" % ulfn)

		response['content_type'] = "application/json"
		response.status = 200
		return factory.toString(who, compact=False)

	def dispatch_views(self):
		self.app.route('/<ulan>', ["get"], self.handle_id)

	def after_request(self):
        # Add CORS and other static headers
		methods = 'PUT, PATCH, GET, POST, DELETE, OPTIONS, HEAD'
		hdrs = 'ETag, Vary, Accept, Prefer, Content-type, Link, Allow, Content-location, Location'
		response.headers['Access-Control-Allow-Origin'] = '*'
		response.headers['Access-Control-Allow-Methods'] = methods
		response.headers['Access-Control-Allow-Headers'] = hdrs
		response.headers['Access-Control-Expose-Headers'] = hdrs
		response.headers['Allow'] = methods
		response.headers['Vary'] = "Accept, Prefer"

	def get_bottle_app(self):
		self.app = Bottle()
		self.dispatch_views()
		#self.app.hook('before_request')(self.before_request)
		self.app.hook('after_request')(self.after_request)
		#self.app.error_handler = self.get_error_handler()
		return self.app        


svc = ULAN_CRM_Server()
#run(host="localhost", port="8888", app=svc.get_bottle_app(), debug=True)
application = svc.get_bottle_app()
