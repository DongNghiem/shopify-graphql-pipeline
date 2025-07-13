from datetime import datetime
class QueryBuilder:
    """
    Each API type will have different query structure. 
    The class helps to instantiate api_type and app id to compose the correct api query to execute. 
    Functions in this class helps building query for API requests.
    """
    def __init__(self, api_type, id):
         self.api_type = api_type
         self.id = id
    #[START get_query]
    def get_query(self, pagination=None, timeMin=None):
        query=None
        current_time=datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        #[START: FOR 'transactions' API type]
        if self.api_type == 'transactions':
            query = f"""
                    query {{
                    transactions(appId: "{self.id}", first:100, createdAtMax:"{current_time}"{timeMin}{pagination}) {{
                        edges {{
                            cursor
                            node {{
                                createdAt
                                id
                                ... on AppSubscriptionSale {{
                                    id
                                    chargeId
                                    createdAt
                                    grossAmount {{
                                        amount
                                        currencyCode
                                    }}
                                    netAmount {{
                                        amount
                                    }}
                                    shop {{
                                        id
                                        myshopifyDomain
                                    }}
                                    shopifyFee {{
                                        amount
                                    }}
                                }}
                                ... on AppOneTimeSale {{
                                    id
                                    chargeId
                                    createdAt
                                    grossAmount {{
                                        amount
                                        currencyCode
                                    }}
                                    netAmount {{
                                        amount
                                    }}
                                    shop {{
                                        id
                                        myshopifyDomain
                                    }}
                                    shopifyFee {{
                                        amount
                                    }}
                                }}
                                ... on AppSaleAdjustment {{
                                    id
                                    chargeId
                                    createdAt
                                    grossAmount {{
                                        amount 
                                        currencyCode
                                    }}
                                    netAmount {{
                                        amount
                                    }}
                                    shop {{
                                        id
                                        myshopifyDomain
                                    }}
                                    shopifyFee {{
                                        amount
                                    }}
                                }}
                                ... on AppSaleCredit {{
                                    id
                                    chargeId
                                    createdAt
                                    grossAmount {{
                                        amount
                                        currencyCode
                                    }}
                                    netAmount {{
                                        amount
                                    }}
                                    shop {{
                                        id
                                        myshopifyDomain
                                    }}
                                    shopifyFee {{
                                        amount
                                    }}
                                }}
                            }}
                        }}
                        pageInfo {{
                            hasNextPage
                        }}
                    }}
                }}
                """
        #[END: FOR 'transactions' API type]

        #[START: FOR 'events' API type]
        if self.api_type == 'events':
                query = f"""
                     query {{
                        app(id: "{self.id}") {{
                        events(first:100, occurredAtMax:"{current_time}"{timeMin}{pagination}) {{
                        edges {{
                            cursor
                            node {{
                            occurredAt
                            type
                            ... on RelationshipDeactivated {{
                                shop {{
                                myshopifyDomain
                                name
                                id
                                }}
                                app {{
                                name
                                }}
                            }}
                            ... on RelationshipInstalled {{
                                shop {{
                                myshopifyDomain
                                name
                                id
                                }}
                                app {{
                                name
                                }}
                            }}
                            ... on RelationshipReactivated {{
                                shop {{
                                myshopifyDomain
                                name
                                id
                                }}
                                app {{
                                name
                                }}
                            }}
                            ... on RelationshipUninstalled {{
                                shop {{
                                myshopifyDomain
                                name
                                id
                                }}
                                reason
                                app {{
                                name
                                }}
                            }}
                            ... on SubscriptionChargeActivated {{
                                shop {{
                                myshopifyDomain
                                name
                                id
                                }}
                                app {{
                                name
                                }}
                                charge {{
                                amount {{
                                    amount
                                    currencyCode
                                }}
                                billingOn
                                id
                                name
                                }}
                            }}
                            ... on SubscriptionChargeCanceled {{
                                shop {{
                                myshopifyDomain
                                name
                                id
                                }}
                                app {{
                                name
                                }}
                                charge {{
                                amount {{
                                    amount
                                    currencyCode
                                }}
                                billingOn
                                id
                                name
                                }}
                            }}
                            ... on SubscriptionChargeDeclined {{
                                shop {{
                                myshopifyDomain
                                name
                                id
                                }}
                                app {{
                                name
                                }}
                                charge {{
                                amount {{
                                    amount
                                    currencyCode
                                }}
                                billingOn
                                id
                                name
                                }}
                            }}
                            ... on SubscriptionChargeExpired {{
                                shop {{
                                myshopifyDomain
                                name
                                id
                                }}
                                app {{
                                name
                                }}
                                charge {{
                                amount {{
                                    amount
                                    currencyCode
                                }}
                                billingOn
                                id
                                name
                                }}
                            }}
                            ... on SubscriptionChargeFrozen {{
                                shop {{
                                myshopifyDomain
                                name
                                id
                                }}
                                app {{
                                name
                                }}
                                charge {{
                                amount {{
                                    amount
                                    currencyCode
                                }}
                                billingOn
                                id
                                name
                                }}
                            }}
                            ... on SubscriptionChargeUnfrozen {{
                                shop {{
                                myshopifyDomain
                                name
                                id
                                }}
                                app {{
                                name
                                }}
                                charge {{
                                amount {{
                                    amount
                                    currencyCode
                                }}
                                billingOn
                                id
                                name
                                }}
                            }}
                            ... on UsageChargeApplied {{
                                shop {{
                                myshopifyDomain
                                name
                                id
                                }}
                                app {{
                                name
                                }}
                                charge {{
                                amount {{
                                    amount
                                    currencyCode
                                }}
                                id
                                name
                                }}
                            }}
                            }}
                        }}
                        pageInfo {{
                            hasNextPage
                        }}
                        }}
                    }}
                    }}
                    """
        #[END: FOR 'events' API type]

        return query
    #[END: get_query]
    