import pandas as pd

dfCustomers = pd.read_csv("../../data/raw/customers.csv")
dfProducts = pd.read_csv("../../data/raw/products.csv")
dfOrders = pd.read_csv("../../data/raw/orders.csv")
dfOrderItems = pd.read_csv("../../data/raw/order_items.csv")
dfDeliveries = pd.read_csv("../../data/raw/deliveries.csv")
dfReturns = pd.read_csv("../../data/raw/returns.csv")

print(dfCustomers.shape)
print(dfProducts.shape)
print(dfOrders.shape)
print(dfOrderItems.shape)
print(dfDeliveries.shape)
print(dfReturns.shape)