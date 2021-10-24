import {DynamoDBClient, paginateScan, BatchWriteItemCommand, TransactWriteItemsCommand, DescribeTableCommand, GetItemCommand} from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import Table from "cli-table3";
import chalk from "chalk";
const {black, bold} = chalk;
import fp  from "lodash/fp.js";
const {flow, map, concat, flatMap, uniq, sortBy} = fp;

const client = new DynamoDBClient({});

const outputs = JSON.parse(process.env.TERRAFORM_OUTPUT);

const GROUP_TABLE = outputs["group-table"].value;
const USER_TABLE = outputs["user-table"].value;

const clearDbs = async () => {
	for await (const page of paginateScan({
		client,
		pageSize: 20,
	}, {
		TableName: USER_TABLE,
	})) {
		if (page.Items.length > 0) {
			await client.send(new BatchWriteItemCommand({
				RequestItems: {
					[USER_TABLE]: page.Items.map(({ID}) => ({
						DeleteRequest: {
							Key: {ID},
						},
					})),
				}
			}));
		}
	}
	for await (const page of paginateScan({
		client,
		pageSize: 20,
	}, {
		TableName: GROUP_TABLE,
	})) {
		if (page.Items.length > 0) {
			await client.send(new BatchWriteItemCommand({
				RequestItems: {
					[GROUP_TABLE]: page.Items.map(({ID}) => ({
						DeleteRequest: {
							Key: {ID},
						},
					})),
				}
			}));
		}
	}
};

const printDb = async () => {
	const printTable = async (tableName) => {
		const items = [];
		for await (const page of paginateScan({
			client,
			pageSize: 20,
		}, {
			TableName: tableName,
		})) {
			for (const item of page.Items) {
				items.push(unmarshall(item));
			}
		}
		const tableInfo = await client.send(new DescribeTableCommand({
			TableName: tableName,
		}));
		const keys = tableInfo.Table.KeySchema;

		const allProperties = flow(
			map(({AttributeName}) => AttributeName),
			concat(
				flatMap((o) => Object.keys(o))(items),
			),
			uniq,
			map((property) => {
				const keySchema = keys.find(({AttributeName}) => AttributeName === property);

				return {
					property: property,
					key: keySchema ? keySchema.KeyType : undefined,
				};
			}),
			sortBy(({key}) => {
				return key ?
					key === "HASH" ? 0 : 1
					: 2;
			}),
		)(keys);
		const headerRow = flow(
			map(({property, key}) => key === "HASH" ? black.bgGreen(` ${property} `) + " (PK)" : key === "RANGE" ? black.bgYellow(` ${property} `)+ " (SK)" : black(property)),
			map((v) => ({
				content: v,
				hAlign: "center",
			})),
		)(allProperties);

		const dataRows = flow(
			map((row) => flow(
				map(({property, key}) => {
					const value = row[property];
					return key ? bold(value) : black(value);
				}),
			)(allProperties)),
		)(items);
		const table = new Table({});
		table.push([{colSpan: headerRow.length, content: tableName, hAlign: "center"}]);
		table.push(headerRow);
		dataRows.forEach((r) => table.push(r));

		console.log(table.toString());
	};

	await printTable(GROUP_TABLE);
	await printTable(USER_TABLE);
};

const insertGroup = (id) => {
	return client.send(new TransactWriteItemsCommand({
		TransactItems: [
			{
				Put: {
					TableName: GROUP_TABLE,
					ConditionExpression: "attribute_not_exists(#pk)",
					ExpressionAttributeNames: {
						"#pk": "ID",
					},
					Item: {
						ID: {S: id},
						num_users: {N: "0"},
					}
				},
			},
		]
	}));
};

const insertUser = (id, group, name) => {
	return client.send(new TransactWriteItemsCommand({
		TransactItems: [
			{
				Put: {
					TableName: USER_TABLE,
					ConditionExpression: "attribute_not_exists(#pk)",
					ExpressionAttributeNames: {
						"#pk": "ID",
					},
					Item: {
						ID: {S: id},
						group: {S: group},
						name: {S: name},
					}
				},
			},
			{
				Update: {
					TableName: GROUP_TABLE,
					UpdateExpression: "ADD #num_users :one",
					ConditionExpression: "attribute_exists(#pk)",
					Key: {
						ID: {S: group},
					},
					ExpressionAttributeNames: {
						"#pk": "ID",
						"#num_users": "num_users",
					},
					ExpressionAttributeValues: {
						":one": {N: "1"},
					}
				},
			}
		]
	}));
};

const updateUser = async (id, group, name) => {
	const currentUser = unmarshall((await client.send(new GetItemCommand({
		TableName: USER_TABLE,
		Key: {
			ID: {S: id}
		}
	}))).Item);

	return client.send(new TransactWriteItemsCommand({
		TransactItems: [
			{
				Put: {
					TableName: USER_TABLE,
					ConditionExpression: "attribute_exists(#pk) AND #group = :group",
					ExpressionAttributeNames: {
						"#pk": "ID",
						"#group": "group",
					},
					ExpressionAttributeValues: {
						":group": {S: currentUser.group},
					},
					Item: {
						ID: {S: id},
						group: {S: group},
						name: {S: name},
					}
				},
			},
			...(currentUser.group !== group ? [
				{
					Update: {
						TableName: GROUP_TABLE,
						UpdateExpression: "ADD #num_users :one",
						ConditionExpression: "attribute_exists(#pk)",
						Key: {
							ID: {S: group},
						},
						ExpressionAttributeNames: {
							"#pk": "ID",
							"#num_users": "num_users",
						},
						ExpressionAttributeValues: {
							":one": {N: "1"},
						}
					},
				},
				{
					Update: {
						TableName: GROUP_TABLE,
						UpdateExpression: "ADD #num_users :minusone",
						ConditionExpression: "attribute_exists(#pk) AND #num_users > :zero",
						Key: {
							ID: {S: currentUser.group},
						},
						ExpressionAttributeNames: {
							"#pk": "ID",
							"#num_users": "num_users",
						},
						ExpressionAttributeValues: {
							":minusone": {N: "-1"},
							":zero": {N: "0"},
						}
					},
				},
			] : [])
		]
	}));
};

const deleteUser = async (id) => {
	const currentUser = unmarshall((await client.send(new GetItemCommand({
		TableName: USER_TABLE,
		Key: {
			ID: {S: id}
		}
	}))).Item);
	return client.send(new TransactWriteItemsCommand({
		TransactItems: [
			{
				Delete: {
					TableName: USER_TABLE,
					ConditionExpression: "attribute_exists(#pk) AND #group = :group",
					ExpressionAttributeNames: {
						"#pk": "ID",
						"#group": "group",
					},
					ExpressionAttributeValues: {
						":group": {S: currentUser.group},
					},
					Key: {
						ID: {S: id}
					},
				},
			},
			{
				Update: {
					TableName: GROUP_TABLE,
					UpdateExpression: "ADD #num_users :minusone",
					ConditionExpression: "attribute_exists(#pk) AND #num_users > :zero",
					Key: {
						ID: {S: currentUser.group},
					},
					ExpressionAttributeNames: {
						"#pk": "ID",
						"#num_users": "num_users",
					},
					ExpressionAttributeValues: {
						":minusone": {N: "-1"},
						":zero": {N: "0"}
					}
				},
			}
		]
	}));
};

const deleteGroup = async (id) => {
	return client.send(new TransactWriteItemsCommand({
		TransactItems: [
			{
				Delete: {
					TableName: GROUP_TABLE,
					ConditionExpression: "attribute_exists(#pk) AND #num_users = :zero",
					ExpressionAttributeNames: {
						"#pk": "ID",
						"#num_users": "num_users",
					},
					ExpressionAttributeValues: {
						":zero": {N: "0"}
					},
					Key: {
						ID: {S: id}
					},
				},
			},
		]
	}));
};

(async () => {
	await clearDbs();

	await insertGroup("group_1");
	await printDb();
	console.log("=====Adding User 1 to Group 1=====");
	await insertUser("user1", "group_1", "User 1");
	await printDb();
	console.log("=====Adding a user to a non-existent group=====");
	try {
		await insertUser("user2", "group_2", "User 2");
	}catch(e) {
		console.log("Failed");
	}
	console.log("=====Adding a second user to group 1=====");
	await insertUser("user2", "group_1", "User 2");
	await printDb();
	console.log("=====Deleting User 2=====");
	await deleteUser("user2");
	await printDb();
	console.log("=====Deleting the group while still having a user=====");
	try {
		await deleteGroup("group_1");
	}catch(e) {
		console.log("Failed");
	}
	console.log("=====Adding a second group and user=====");
	await insertUser("user2", "group_1", "User 2");
	await insertGroup("group_2");
	await printDb();
	console.log("=====Moving the second user to the second group=====");
	await updateUser("user2", "group_2", "User 2");
	await printDb();
	console.log("=====Updating a user without changing the group=====");
	await updateUser("user2", "group_2", "User 2_b");
	await printDb();
})();
