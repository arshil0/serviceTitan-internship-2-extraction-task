import pickle #the pickle library to import a pkl file and open it
import pandas as pd #the most common python library for dealing with data using dataframes

class CustomerDataExtractor:
    def __init__(self, dataFile, vip_customers = None):
        
        with open(dataFile, "rb") as f:
            data = pickle.load(f)

        #if a list of vip customer id's is given, read it and convert it to a list to then check which customers are vip
        if vip_customers != None:
            with open(vip_customers, "r") as v:
                #after converting the id's into a list
                vips = v.read().split("\n")
                #convert the "string" numbers into integers, to compare user id's more easily later and save it inside of "self"
                self.vip_list = [int(id) for id in vips if id != ""]

        self.dataframe = pd.DataFrame(data)
        #print(self.dataframe["orders"][0])
        #self.dataframe.to_csv("original_data.csv")


    def generate_dataframe(self):
        #this is the final dataframe output that will be returned by this class
        final_dataframe = {
            "customer_id" : [],
            "customer_name" : [],
            "registration_date" : [],
            "is_vip": [],
            "order_id" : [],
            "order_date" : [],
            "product_id" : [],
            "product_name" : [],
            "category" : [],
            "unit_price" : [],
            "item_quantity" : [],
            "total_item_price" : [],
            "total_order_value_percentage" : []
        }


        #iterate through all data points row by row
        for data_index in range(len(self.dataframe)):
            #unfortunately I don't think I can iterate instantly row by row, I have to go column by column.
            for column in self.dataframe:
                #print(column)
                #print(self.dataframe[column][data_index])

                #the "id" and "name" columns didn't seem to have any input errors in the original data, we can simply add them as they are in the final dataframe variable
                if column == "id":
                    data = self.dataframe[column][data_index]
                    final_dataframe["customer_id"].append(data)

                    #check if the current customer is a vip
                    if data in self.vip_list:
                        final_dataframe["is_vip"].append(True)
                    else:
                        final_dataframe["is_vip"].append(False)

                elif column == "name":
                    final_dataframe["customer_name"].append(self.dataframe[column][data_index])


                elif column == "registration_date":
                    #got this function "to_datetime" by gemini while trying to understand what datetime64 ns is exactly
                    #it's also convenient, as if the data is missing, it returns "None", and there is 1 row with a missing registration date
                    final_dataframe["registration_date"].append(pd.to_datetime(self.dataframe[column][data_index]))



                #now onto the list of orders for each customer
                #some customers don't have orders, if not, we can simply add "None" to that customer's data related with orders and items
                elif column == "orders":
                    #I am using this variable to add a new row with the same customer values (id, name, etc...) if they have more than 1 order / item ordered.
                    #if this is true, skip adding a new row for that same customer on the first order, and add a new row for the rest
                    skip_first_customer_addition = True
                    added_customer_data = False
                    #this variable is the same as the above variable but for items, if it's the first item no need to add a new row
                    skip_first_order_addition = True


                    orders = self.dataframe[column][data_index]
                    #if this customer has no orders, then add "None" to the remaining data attributes
                    if type(orders) != list or len(orders) == 0:
                        final_dataframe["order_id"].append("None")
                        final_dataframe["order_date"].append("None")
                        final_dataframe["product_id"].append("None")
                        final_dataframe["product_name"].append("None")
                        final_dataframe["category"].append("None")
                        final_dataframe["unit_price"].append("None")
                        final_dataframe["item_quantity"].append("None")
                        final_dataframe["total_item_price"].append("None")
                        final_dataframe["total_order_value_percentage"].append("None")

                    else:
                        for order in orders:
                            if not skip_first_customer_addition:
                                #re-add the same customer data before adding the new order data
                                final_dataframe["customer_id"].append(final_dataframe["customer_id"][-1])
                                final_dataframe["customer_name"].append(final_dataframe["customer_name"][-1])
                                final_dataframe["registration_date"].append(final_dataframe["registration_date"][-1])
                                final_dataframe["is_vip"].append(final_dataframe["is_vip"][-1])
                                added_customer_data = True

                            print(order)
                            final_dataframe["order_id"].append(order["order_id"])
                            #here I am using dayfirst and yearfirst, as the formatting is different in the given data
                            final_dataframe["order_date"].append(pd.to_datetime(order["order_date"], yearfirst=True, dayfirst = True))


                            #now, check if the order has any items
                            items = order.get("items")
                            if items != None and len(items) > 0:
                                for item in items:
                                    if not skip_first_order_addition and not added_customer_data:
                                        #re-add the same customer data before adding the new order data
                                        final_dataframe["customer_id"].append(final_dataframe["customer_id"][-1])
                                        final_dataframe["customer_name"].append(final_dataframe["customer_name"][-1])
                                        final_dataframe["registration_date"].append(final_dataframe["registration_date"][-1])
                                        final_dataframe["is_vip"].append(final_dataframe["is_vip"][-1])
                                        final_dataframe["order_id"].append(order["order_id"])
                                        final_dataframe["order_date"].append(pd.to_datetime(order["order_date"], yearfirst=True, dayfirst = True))
                                    
                                    added_customer_data = False
                                    final_dataframe["product_id"].append(item["item_id"])
                                    final_dataframe["product_name"].append(item["product_name"])
                                    final_dataframe["category"].append(item["category"])

                                    #some prices had "$" at the beginning, if that's the case remove it so it can be turned into a float
                                    price = item["price"]
                                    if price == None:
                                        final_dataframe["unit_price"].append(0)
                                        final_dataframe["item_quantity"].append(0)
                                        final_dataframe["total_item_price"].append(0)

                                        
                                    else:
                                        if type(price) != float and "$" in price:
                                            price = float(price[1:-1])
                                        final_dataframe["unit_price"].append(price)

                                        quantity = item["quantity"]

                                        #some data points had "FREE" as their value, I am counting them as 0
                                        if quantity != "FREE":
                                            final_dataframe["item_quantity"].append(item["quantity"])
                                            final_dataframe["total_item_price"].append(price * int(quantity))
                                        else:
                                            final_dataframe["item_quantity"].append(0)
                                            final_dataframe["total_item_price"].append(0)
                                    
                                    #I unfortunately didn't have time to fix this, I had problems with this, due to some bad data and I didn't have time to clean it
                                    """
                                    
                                    if float(order["order_total_value"]) > 0:
                                        final_dataframe["total_order_value_percentage"].append(price * int(quantity) / float(order["order_total_value"]))
                                    else:
                                        final_dataframe["total_order_value_percentage"].append(0)
                                    """

                                    final_dataframe["total_order_value_percentage"].append("None")

                                    skip_first_order_addition = False

                            else:
                                final_dataframe["product_id"].append("None")
                                final_dataframe["product_name"].append("None")
                                final_dataframe["category"].append("None")
                                final_dataframe["unit_price"].append("None")
                                final_dataframe["item_quantity"].append("None")
                                final_dataframe["total_item_price"].append("None")
                                final_dataframe["total_order_value_percentage"].append("None")
                            


                            skip_first_customer_addition = False

                        """
                        
                        final_dataframe["product_id"].append("None")
                        final_dataframe["product_name"].append("None")
                        final_dataframe["category"].append("None")
                        final_dataframe["unit_price"].append("None")
                        final_dataframe["item_quantity"].append("None")
                        final_dataframe["total_item_price"].append("None")
                        final_dataframe["total_order_value_percentage"].append("None")
                        """

        
        #print(final_dataframe)
        #print(len(final_dataframe["customer_id"]))
        #print(len(final_dataframe["order_id"]))
        #print(len(final_dataframe["product_id"]))
        #print(len(final_dataframe["item_quantity"]))
        pd.DataFrame(final_dataframe).to_csv("extracted_data.csv")
                


extractor = CustomerDataExtractor("customer_orders.pkl", "vip_customers.txt")
extractor.generate_dataframe()