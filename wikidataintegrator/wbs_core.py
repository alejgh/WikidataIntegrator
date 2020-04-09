from pyshex.utils.schema_loader import SchemaLoader
from wikidataintegrator import wdi_core
import json
import pdb
import requests

"""
Authors:
  Andra Waagmeester (andra' at ' micelio.be)

This file is part of the WikidataIntegrator.

"""
__author__ = 'Andra Waagmeester'
__license__ = 'MIT'

# TODO: move this to class
INVALID_PROP = -1

def visit_shape_expression(source_schema, callback_map):
    def _visit_shape_expression_node(node, context):
        if 'type' in node and node['type'] in callback_map:
            callback_map[node['type']](node, context)

        for v in node.values():
            if isinstance(v, dict):
                _visit_shape_expression_node(v, context)
            elif isinstance(v, list):
                for child in v:
                    if isinstance(child, dict):
                        _visit_shape_expression_node(child, context)

    context = {}
    _visit_shape_expression_node(source_schema, context)
    return context

class SubsetExtractorVisitor():

    def __init__(self, subset_engine):
        self.current_predicate = ""
        self.item_json = {}
        self.shapes_ids = []
        self.subset_engine = subset_engine
        self.subset = {}
        self.source_schema = {}
        self.languages = []

    def extract_subset_from_shape(self, item_json, source_schema, shape_id,
                                  languages, current_predicate, subset):
        self.current_predicate = current_predicate
        self.subset = subset
        self.shapes_ids = [shape["id"] for shape in source_schema["shapes"] if shape["type"] == "Shape"]
        self.source_schema = source_schema
        self.languages = languages
        self.item_json = item_json

        callback_map = {
            "TripleConstraint": self._on_triple_constraint,
            "NodeConstraint": self._on_node_constraint,
            "ShapeOr": self._on_shape_or
        }

        for shape in source_schema["shapes"]:
            if shape["id"] == shape_id:
                visit_shape_expression(shape, callback_map)
        return self.subset

    @property
    def prop_id(self):
        return self.current_predicate.split('/')[-1]

    def _is_predicate_p(self):
        return 'prop/P' in self.current_predicate

    def _is_predicate_pr(self):
        return 'prop/reference' in self.current_predicate

    def _is_predicate_prov(self):
        return 'prov' in self.current_predicate

    def _is_predicate_ps(self):
        return 'prop/statement' in self.current_predicate

    def _is_predicate_pq(self):
        return 'prop/qualifier' in self.current_predicate

    def _is_predicate_wdt(self):
        return 'prop/direct' in self.current_predicate

    def _on_node_constraint(self, node, context):
        # we copy everything
        self._allow_all_values_of_property(self.prop_id)

    def _on_shape_or(self, node, context):
        #pdb.set_trace()
        filtered_shapes = list(filter(lambda shape: shape in self.shapes_ids, node["shapeExprs"]))
        if self._is_predicate_wdt() or self._is_predicate_ps(): # wdt or ps
            if self.prop_id not in self.item_json['claims']:
                return

            for shape in filtered_shapes:
                self._on_entity_shape_expr(self.item_json['claims'][self.prop_id], shape)
        elif self._is_predicate_p(): # p
            if self.prop_id not in self.item_json['claims']:
                return

            self.subset[self.prop_id] = {'references': [], 'qualifiers': []}
            for shape in filtered_shapes:
                tmp_subset = {'references': [], 'qualifiers': []}
                SubsetExtractorVisitor(self.subset_engine).\
                    extract_subset_from_shape(self.item_json['claims'][self.prop_id],
                                              self.source_schema, shape, self.languages,
                                              self.current_predicate,
                                              tmp_subset)
                self.subset['references'] += tmp_subset['references']
                self.subset['qualifiers'] += tmp_subset['qualifiers']
        elif self._is_predicate_pq(): # pq
            self.subset["qualifiers"].append(self.prop_id)
            if 'qualifiers' not in self.item_json \
               or self.prop_id not in self.item_json['qualifiers']:
                return

            for shape in filtered_shapes:
                self._on_entity_shape_expr(self.item_json['qualifiers'][self.prop_id],
                                           shape)
        elif self._is_predicate_prov(): # prov
            for shape in filtered_shapes:
                new_references = []
                SubsetExtractorVisitor(self.subset_engine).\
                    extract_subset_from_shape(self.item_json, self.source_schema, shape,
                                              self.languages, self.current_predicate,
                                              new_references)
                self.subset["references"] += new_references
        elif self._is_predicate_pr(): # pr
            self.subset["references"].append(self.prop_id)
            if 'references' not in self.item_json \
               or self.prop_id not in self.item_json['references']['snaks']:
                return

            for shape in filtered_shapes:
                self._on_entity_shape_expr(self.item_json['references']['snaks'][self.prop_id],
                                           shape)

    def _on_triple_constraint(self, node, context):
        self.current_predicate = node['predicate']
        #pdb.set_trace()
        if not self.prop_id.startswith('P') and not self._is_predicate_prov():
            return

        if 'valueExpr' not in node:
            # all values allowed
            self._allow_all_values_of_property(self.prop_id)
            return

        value_expression = node['valueExpr']
        if not isinstance(value_expression, str):
            return

        if value_expression in self.shapes_ids:
            if self._is_predicate_wdt() or self._is_predicate_ps(): # wdt or ps
                if self.prop_id not in self.item_json['claims']:
                    return
                self._on_entity_shape_expr(self.item_json['claims'][self.prop_id], value_expression)
            elif self._is_predicate_p(): # p
                if self.prop_id not in self.item_json['claims']:
                    return

                self.subset[self.prop_id] = {'references': [], 'qualifiers': []}
                SubsetExtractorVisitor(self.subset_engine).\
                    extract_subset_from_shape(self.item_json['claims'][self.prop_id],
                                              self.source_schema, value_expression,
                                              self.languages, self.current_predicate,
                                              self.subset[self.prop_id])
            elif self._is_predicate_pq(): # pq
                self.subset["qualifiers"].append(self.prop_id)
                if 'qualifiers' not in self.item_json \
                   or self.prop_id not in self.item_json['qualifiers']:
                    return
                self._on_entity_shape_expr(self.item_json['qualifiers'][self.prop_id],
                                           value_expression)
            elif self._is_predicate_prov(): # prov
                SubsetExtractorVisitor(self.subset_engine).\
                    extract_subset_from_shape(self.item_json, self.source_schema, value_expression,
                                              self.languages, self.current_predicate,
                                              self.subset)
            elif self._is_predicate_pr(): # pr
                self.subset["references"].append(self.prop_id)
                if 'references' not in self.item_json \
                   or self.prop_id not in self.item_json['references']['snaks']:
                    return
                self._on_entity_shape_expr(self.item_json['references']['snaks'][self.prop_id],
                                           value_expression)
        else:
            # value expression pointing to node constraint, all values allowed
            self._allow_all_values_of_property(self.prop_id)
        print(f"TripleConstraint - Pred: {self.current_predicate} - Value: {value_expression}")

    def _on_entity_shape_expr(self, values, shape_expression):
        for value in values:
            next_node = value['mainsnak']['datavalue']['value']['id']
            if next_node not in self.subset_engine.nodes_to_copy:
                self.subset_engine.copy_node_to_targetwb(next_node, self.source_schema,
                                                         shape_expression, self.languages)

    def _allow_all_values_of_property(self, property_id):
        if self._is_predicate_wdt() or self._is_predicate_p(): #wdt or p
            self.subset[property_id] = {}
        elif self._is_predicate_pq(): # pq
            self.subset["qualifiers"].append(property_id)
        elif self._is_predicate_pr(): # pr
            self.subset["references"].append(property_id)

class WikibaseEngine(object):

    MAPPINGS_PROP_LABEL = "wbs_core mapping"
    MAPPINGS_PROP_DESC = "Mapping to the source entity created automatically by the wbs_core module of wdi."

    def __init__(self, wbsource_url, wbsource_sparql_endpoint, target_login,
                 wbtarget_url, wbtarget_sparql_endpoint):
        """
        constructor
        :param wikibase_url: The base url of the wikibase being accessed (e.g. for wikidata https://www.wikidata.org
        """
        self.wbsource_url = wbsource_url
        self.wbsource_sparql_endpoint = wbsource_sparql_endpoint
        self.wbtarget_url = wbtarget_url
        self.wbtarget_api = wbtarget_url + "/w/api.php"
        self.wbtarget_sparql = wbtarget_sparql_endpoint
        self.login = target_login
        self.local_item_engine = wdi_core.WDItemEngine.wikibase_item_engine_factory(
            self.wbtarget_api, self.wbtarget_sparql)
        self.mappings_cache = {} # maps entity ids between source and target wibase
        self.nodes_to_copy = []
        self._mappings_prop = self._get_or_create_mappings_prop()
        self._load_mappings()

    @classmethod
    def extractPropertiesFrom(cls, schema_json):
        properties = []

        def parse_predicate(node, context):
            value = node["predicate"]
            if "label" not in value and "description" not in value:
                # extract "PXX" from complete url
                property_tokens = value.split("/")
                if property_tokens[-1].startswith("P"):
                    properties.append(property_tokens[-1])

        callback_map = {
            "TripleConstraint": parse_predicate
        }
        visit_shape_expression(schema_json, callback_map)
        return list(set(properties))

    @classmethod
    def extractItemsFrom(cls, schema_json):
        items = []
        def parse_values(node, context):
            if 'values' not in node:
                return

            values = node['values']
            for val in values:
                item_tokens = val.split('/')
                if item_tokens[-1].startswith('Q'):
                    items.append(item_tokens[-1])

        callback_map = {
            "NodeConstraint": parse_values
        }
        visit_shape_expression(schema_json, callback_map)
        return list(set(items))

    def create_subset_from(self, source_schema, sparql_query, languages=["en", "nl"]):
        """

        :param login: An object of type WDLogin, which holds the credentials of the target wikibase instance.
        :type login: wdi_login.WDLogin
        :param wikibase_source: Base URL pointing to the source wikibase instance where the properties are stored.
        :type wikibase_source: str
        :param source_schema: URL pointing to an entity schema from which the properties
            will be obtained (e.g. https://www.wikidata.org/wiki/Special:EntitySchemaText/E37).
        :type source_schema: str
        :param languages: List of languages the labels and descriptions of the properties in the target wikibase.
        :type languages: List[str]
        """
        model_json = self._load_schema(source_schema)

        #self.copyItems(model_json, languages)
        #self.copyProperties(model_json, languages)

        # obtain nodes from query -> nodes
        nodes = []
        for result in wdi_core.WDItemEngine.execute_sparql_query(sparql_query, endpoint=self.wbsource_sparql_endpoint)["results"]["bindings"]:
            nodes.append(result["item"]["value"].replace(f"{self.wbsource_url}/entity/", ""))

        pdb.set_trace()
        # TODO: validate nodes against given schema -> validated nodes
        #validated_nodes = [node for node in nodes if node._conforms_to_shape(source_schema)]

        final_mappings = {}
        # copy each validated node to the target wb, writting just the properties and values that appear in the ShEx
        for node in nodes:
            target_id = self.copy_node_to_targetwb(node, model_json, model_json["start"], languages)
            final_mappings[node] = target_id

        print('Final mappings')
        for k, v in final_mappings.items():
            print(f"{k} ---> {v}.")

    def copy_node_to_targetwb(self, node, source_schema, shape_id, languages):
        # TODO: validate nodo contra la shape, al hacerlo aqui permitimos recursividad en los shapeOr
        self.nodes_to_copy.append(node)
        print(f"Copying entity {node} with shape '{shape_id}'")
        item_json = self._load_entity_json(self.wbsource_url, node, languages)['entities'][node]
        subset = {}
        subset_extractor = SubsetExtractorVisitor(self)
        subset_extractor.extract_subset_from_shape(item_json, source_schema, shape_id,
                                                   languages, "", subset)
        #pdb.set_trace()
        item_id = self.createItem(item_json, languages, deep_copy=True, subset=subset)
        print(f"Finished. Target id of {node} -> {item_id}")
        self.nodes_to_copy.clear()
        return item_id


    def copyItems(self, schema_json, languages):
        items = self.extractItemsFrom(schema_json)
        for item_id in items:
            print(item_id)
            page = json.loads(requests.get(self.wbsource_url + \
                              "/w/api.php?action=wbgetentities&format=json&ids=" + item_id).text)
            target_id = self.createItem(page['entities'][item_id], languages)
            print(f"Target ID -> {target_id}")


    def copyProperties(self, schema_json, languages):
        """
        Copy the properties from a wikibase instance to another using a ShEx schema.

        :param login: An object of type WDLogin, which holds the credentials of the target wikibase instance.
        :type login: wdi_login.WDLogin
        :param wikibase_source: Base URL pointing to the source wikibase instance where the properties are stored.
        :type wikibase_source: str
        :param source_schema: URL pointing to an entity schema from which the properties
            will be obtained (e.g. https://www.wikidata.org/wiki/Special:EntitySchemaText/E37).
        :type source_schema: str
        :param languages: List of languages the labels and descriptions of the properties in the target wikibase.
        :type languages: List[str]
        """
        properties = self.extractPropertiesFrom(schema_json)
        for prop_id in properties:
            print(prop_id)
            page = json.loads(requests.get(self.wbsource_url + \
                              "/w/api.php?action=wbgetentities&format=json&ids=" + prop_id).text)
            target_id = self.createProperty(page['entities'][prop_id], languages)
            print(f"Target ID -> {target_id}")

    def createItem(self, wd_json, languages, deep_copy=True, subset=None):
        """ Creates an item in the target wikibase """
        return self.createEntity(wd_json, languages, deep_copy, etype='item', subset=subset)

    def createProperty(self, wd_json, languages, deep_copy=True, subset=None):
        property_datatype = wd_json['datatype']
        return self.createEntity(wd_json, languages, deep_copy,
                                 etype='property', subset=subset, property_datatype=property_datatype)

    def createEntity(self, wd_json, languages, deep_copy, etype, subset, **kwargs):

        claims, labels, descriptions, source_id = (wd_json['claims'], wd_json['labels'],
                                                   wd_json['descriptions'], wd_json['id'])
        print(f"Create entity: {source_id}")
        if self.existsEntity(source_id, etype):
            target_id = self._get_target_id_of(source_id, etype)
            print(f"{source_id} already exists -> {target_id}")
            if deep_copy:
                # if we are making a deep copy, we overwrite the statements with the new ones
                item = self.local_item_engine(wd_item_id=target_id)
                self._add_statements_to_item(item, etype, claims, languages, subset, overwrite=True)
            return target_id

        item = self.local_item_engine(new_item=True)
        if 'en' not in languages:
            languages.append('en')

        for language in languages:
            if language in labels.keys():
                item.set_label(labels[language]["value"], lang=language)
            if language in descriptions.keys():
                item.set_description(descriptions[language]["value"][:250], lang=language)

        try:
            new_item_id = item.write(self.login, entity_type=etype, **kwargs)
        except wdi_core.WDApiError as e:
            pdb.set_trace()
            if 'wikibase-api-not-recognized-datatype' in e.wd_error_msg['error']['messages'][0]['name']:
                self.mappings_cache[source_id] = INVALID_PROP
                print(f"Not supported datatype for property '{source_id}' from source.")
            return INVALID_PROP

        self._add_mapping_to_item(item, source_id, etype)
        self.mappings_cache[source_id] = new_item_id # update mappings cache

        if len(claims) == 0 or not deep_copy:
            # no statements to add, we can finish
            return new_item_id

        self._add_statements_to_item(item, etype, claims, languages, subset)
        return new_item_id


    def existsEntity(self, source_prop_id, etype):
        return self._get_target_id_of(source_prop_id, etype) is not None

    def existsProperty(self, source_prop_id):
        return self._get_target_id_of(source_prop_id, etype='property') is not None

    def existsItem(self, source_item_id):
        return self._get_target_id_of(source_item_id, etype='item') is not None

    def _add_mapping_to_item(self, item, source_id, etype):
        wd_mapping = wdi_core.WDUrl(value=f"{self.wbsource_url}/entity/"+source_id, prop_nr=self._mappings_prop)
        item.update([wd_mapping], append_value=[self._mappings_prop])
        item.write(self.login, entity_type=etype)

    def _add_statements_to_item(self, item, etype, claims, languages, subset, overwrite=False):
        item_data = []
        append_value = []
        for claim_id, claim_data in claims.items():
            if not self._is_prop_in_subset(claim_id, subset):
                continue

            append_value.append(claim_id)
            claim_prop_json = self._load_entity_json(self.wbsource_url, claim_id,
                                                     languages)['entities'][claim_id]
            prop_target_id = self.createProperty(claim_prop_json,
                                                 languages, deep_copy=False)
            if prop_target_id == INVALID_PROP:
                # invalid property (datatype not supported), skip
                continue

            for data in claim_data:
                # qualifiers
                qualifiers_subset = subset[claim_id]["qualifiers"] if subset is not None and \
                    "qualifiers" in subset[claim_id] else None
                item_qualifiers = self._get_qualifiers_from(data, languages, qualifiers_subset)

                # References
                references_subset = subset[claim_id]["references"] if subset is not None and \
                    "references" in subset[claim_id] else None
                item_references = self._get_references_from(data, languages, references_subset)

                mainsnak = data["mainsnak"]
                statement = self._create_statement(languages,
                                                   mainsnak, prop_target_id,
                                                   qualifiers=item_qualifiers,
                                                   references=item_references)
                if statement is not None:
                    item_data.append(statement)

        # TODO: use overwrite parameter to change append_value?
        item.update(item_data, append_value)
        try:
            item.write(self.login, entity_type=etype)
        except wdi_core.WDApiError as e:
            pdb.set_trace()
            return INVALID_PROP

    def _create_statement(self, languages, mainsnak, prop_id, **kwargs):
        if mainsnak['snaktype'] in ['novalue', 'somevalue']:
            # this is a temporal fix
            # TODO: handle novalues and somevalues
            return None

        datatype = mainsnak['datatype']
        datavalue = mainsnak['datavalue']

        if datatype == 'wikibase-item':
            source_item_id = datavalue['value']['id']
            source_item_json = self._load_entity_json(self.wbsource_url, source_item_id,
                                                      languages)['entities'][source_item_id]
            target_item_id = self.createItem(source_item_json,
                                             languages, deep_copy=False)
            return wdi_core.WDItemID(value=target_item_id, prop_nr=prop_id, **kwargs)
        elif datatype == 'wikibase-property':
            source_prop_id = datavalue['value']['id']
            source_prop_json = self._load_entity_json(self.wbsource_url, source_prop_id,
                                                      languages)['entities'][source_prop_id]
            target_prop_id = self.createProperty(source_prop_json,
                                                 languages, deep_copy=False)
            if target_prop_id == INVALID_PROP:
                return None
            return wdi_core.WDProperty(value=target_prop_id, prop_nr=prop_id, **kwargs)
        elif datatype == "time":
            return wdi_core.WDTime(time=datavalue["value"]["time"], prop_nr=prop_id,
                                   precision=datavalue["value"]["precision"],
                                   timezone=datavalue["value"]["timezone"], **kwargs)
        elif datatype == "monolingualtext":
            lang = datavalue["value"]["language"]
            # TODO: fix errors when language is not allowed in target wikibase
            # temporal fix
            if lang not in languages:
                return None

            return wdi_core.WDMonolingualText(value=datavalue["value"]["text"], prop_nr=prop_id,
                                              language=lang, **kwargs)
        elif datatype == "external-id":
            return wdi_core.WDExternalID(value=datavalue["value"], prop_nr=prop_id, **kwargs)
        elif datatype == "globe-coordinate":
            return wdi_core.WDGlobeCoordinate(latitude=datavalue["latitude"],
                                              longitude=datavalue["longitude"],
                                              precision=datavalue["precision"],
                                              prop_nr=prop_id, **kwargs)
        elif datatype == "string":
            return wdi_core.WDString(value=datavalue["value"], prop_nr=prop_id, **kwargs)
        elif datatype == "url":
            return wdi_core.WDUrl(value=datavalue["value"], prop_nr=prop_id, **kwargs)
        elif datatype == "commonsMedia":
            return wdi_core.WDCommonsMedia(value=datavalue["value"], prop_nr=prop_id, **kwargs)
        elif datatype == "geo-shape":
            return wdi_core.WDGeoShape(value=datavalue["value"], prop_nr=prop_id, **kwargs)
        elif datatype == "quantity":
            # TODO: add units to quantity
            return wdi_core.WDQuantity(value=datavalue["value"]["amount"], prop_nr=prop_id, **kwargs)
        else:
            print(f"Not recognized datatype '{datatype}'. Returning None...")
            return None

    def _get_or_create_mappings_prop(self):
        query_res = json.loads(requests.get(f"{self.wbtarget_url}/w/api.php?action=wbsearchentities" + \
            f"&search={self.MAPPINGS_PROP_LABEL}&format=json&language=en&type=property").text)
        if 'search' in query_res and len(query_res['search']) > 0:
            for search_result in query_res['search']:
                if search_result['label'] == self.MAPPINGS_PROP_LABEL and \
                   search_result['description'] == self.MAPPINGS_PROP_DESC:
                    self._mappings_prop = search_result['id']
                    break
        else:
            mappings_item = self.local_item_engine(new_item=True)
            mappings_item.set_label(self.MAPPINGS_PROP_LABEL, lang='en')
            mappings_item.set_description(self.MAPPINGS_PROP_DESC, lang='en')
            self._mappings_prop = mappings_item.write(self.login, entity_type='property',
                                                      property_datatype='url')
        return self._mappings_prop

    def _get_references_from(self, data, languages, subset):
        item_references = []
        if 'references' not in data:
            return item_references

        for reference in data["references"]:
            for reference_id, reference_data in reference["snaks"].items():
                if not self._is_prop_in_subset(reference_id, subset):
                    continue

                reference_prop_json = self._load_entity_json(self.wbsource_url, reference_id,
                                                             languages)['entities'][reference_id]
                reference_target_id = self.createProperty(reference_prop_json,
                                                          languages, deep_copy=False)
                if reference_target_id == INVALID_PROP:
                    continue

                reference_statements = [self._create_statement(languages, value,
                                                               reference_target_id, is_reference=True)
                                        for value in reference_data]
                reference_statements = list(filter(None, reference_statements))
                item_references += [reference_statements]
        return item_references


    def _get_qualifiers_from(self, data, languages, subset):
        item_qualifiers = []
        if 'qualifiers' not in data:
            return item_qualifiers

        for qualifier_id, qualifier_data in data["qualifiers"].items():
            if not self._is_prop_in_subset(qualifier_id, subset):
                continue

            qualifier_prop_json = self._load_entity_json(self.wbsource_url, qualifier_id,
                                                         languages)['entities'][qualifier_id]
            qualifier_target_id = self.createProperty(qualifier_prop_json,
                                                      languages, deep_copy=False)
            if qualifier_target_id == INVALID_PROP:
                continue
            item_qualifiers += [self._create_statement(languages, value,
                                                       qualifier_target_id,
                                                       is_qualifier=True)
                                for value in qualifier_data]
        item_qualifiers = list(filter(None, item_qualifiers))
        return item_qualifiers

    def _get_target_id_of(self, source_item_id, etype, language='en'):
        return self.mappings_cache[source_item_id] if source_item_id in self.mappings_cache else None

    def _load_mappings(self):
        query = f"PREFIX wdt: <{self.wbtarget_url}prop/direct/>"
        query += "SELECT ?target ?source WHERE { ?target wdt:" + self._mappings_prop + " ?source . }"
        for result in wdi_core.WDItemEngine.execute_sparql_query(query, endpoint=self.wbtarget_sparql)["results"]["bindings"]:
            source_id = result["source"]["value"].replace(f"{self.wbtarget_url}/entity", "")
            target_id = result["target"]["value"].replace(f"{self.wbsource_url}/entity", "")
            self.mappings_cache[source_id] = target_id


    def getNamespace(self, nsName):
        namespaces = json.loads(
            requests.get(self.wbtarget_api + "?action=query&format=json&meta=siteinfo&formatversion=2&siprop=namespaces").text)
        for namespace in namespaces["query"]["namespaces"].keys():
            if namespaces["query"]["namespaces"][namespace]["name"] == nsName:
                return namespace

    def listProperties(self):
        """
        List the properties of the target wikibase instance.

        :returns: List of labels of each property in the wikibase.
        :rtype: List[str]
        """
        property_labels = []
        ns = self.getNamespace("Property")
        query_url = self._build_list_properties_query(ns)
        properties = json.loads(requests.get(query_url).text)
        if 'query' not in properties:
            # wikibase is empty
            return []

        self._extract_labels_from_properties(properties, property_labels)
        while 'continue' in properties:
            gapcontinue = properties['continue']['gapcontinue']
            query_url = self._build_list_properties_query(ns, gapcontinue)
            properties = json.loads(requests.get(query_url).text)
            self._extract_labels_from_properties(properties, property_labels)

        return property_labels

    def _build_list_properties_query(self, ns, gapcontinue=None):
        res = [self.wbtarget_api,
               "?action=query&format=json&prop=pageterms&generator=allpages&wbptterms" \
               "=label&gapnamespace=",
               ns]
        if gapcontinue is not None:
            res.append("&gapcontinue=")
            res.append(gapcontinue)
        return ''.join(res)

    def _extract_labels_from_properties(self, properties, propertyLabels):
        for prop in properties["query"]["pages"].values():
            for label in prop["terms"]["label"]:
                propertyLabels.append(label)

    def _is_prop_in_subset(self, prop_id, subset):
        if subset is None:
            return True

        if isinstance(subset, dict) and len(subset) == 0:
            return True

        return prop_id in subset

    def _load_entity_json(self, wikibase, entity_ids, languages):
        return json.loads(requests.get(f"{wikibase}/w/api.php?action=wbgetentities&ids={entity_ids}" + \
            f"&format=json&languages={'|'.join(languages)}").text)

    def _load_schema(self, schema_url):
        loader = SchemaLoader()
        shex = requests.get(schema_url).text
        schema = loader.loads(shex)
        return json.loads(schema._as_json_dumps())
