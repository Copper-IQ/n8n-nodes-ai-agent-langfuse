import type { StreamEvent } from '@langchain/core/dist/tracers/event_stream';
import type { IterableReadableStream } from '@langchain/core/dist/utils/stream';
import type { BaseChatModel } from '@langchain/core/language_models/chat_models';
import type { AIMessageChunk, BaseMessage, MessageContentText } from '@langchain/core/messages';
import { AIMessage, trimMessages } from '@langchain/core/messages';
import type { ToolCall } from '@langchain/core/messages/tool';
import type { ChatPromptTemplate } from '@langchain/core/prompts';
import { RunnableSequence } from '@langchain/core/runnables';
import { type AgentRunnableSequence, createToolCallingAgent } from 'langchain/agents';
import type { BaseChatMemory } from 'langchain/memory';
import type { DynamicStructuredTool, Tool } from 'langchain/tools';
import omit from 'lodash/omit';
import {
	jsonParse,
	NodeConnectionTypes,
	nodeNameToToolName,
	NodeOperationError,
	sleep,
} from 'n8n-workflow';
import type {
	EngineRequest,
	GenericValue,
	IDataObject,
	IExecuteFunctions,
	INodeExecutionData,
	ISupplyDataFunctions,
	EngineResponse,
} from 'n8n-workflow';
import assert from 'node:assert';

import { CallbackHandler } from 'langfuse-langchain';
import { propagateAttributes } from '@langfuse/tracing';
import Langfuse from 'langfuse';

import { getPromptInputByType } from '../src/utils/helpers';
import {
	getOptionalOutputParser,
	type N8nOutputParser,
} from '../src/utils/N8nOutputParser';

import {
	fixEmptyContentMessage,
	getAgentStepsParser,
	getChatModel,
	getOptionalMemory,
	getTools,
	prepareMessages,
	preparePrompt,
} from '../src/utils/common';
import { SYSTEM_MESSAGE } from '../src/utils/prompt';

type ToolCallRequest = {
	tool: string;
	toolInput: Record<string, unknown>;
	toolCallId: string;
	type?: string;
	log?: string;
	messageLog?: unknown[];
};

async function createEngineRequests(
	toolCalls: ToolCallRequest[],
	itemIndex: number,
	tools: Array<DynamicStructuredTool | Tool>,
) {
	return toolCalls.map((toolCall) => {
		// First try to get from metadata (for toolkit tools)
		const foundTool = tools.find((tool) => tool.name === toolCall.tool);

		if (!foundTool) return;

		const nodeName = foundTool.metadata?.sourceNodeName;

		// For toolkit tools, include the tool name so the node knows which tool to execute
		const input = foundTool.metadata?.isFromToolkit
			? { ...toolCall.toolInput, tool: toolCall.tool }
			: toolCall.toolInput;

		return {
			nodeName,
			input,
			type: NodeConnectionTypes.AiTool,
			id: toolCall.toolCallId,
			metadata: {
				itemIndex,
			},
		};
	});
}

/**
 * Uses provided tools and tried to get tools from model metadata
 * Some chat model nodes can define built-in tools in their metadata
 */
function getAllTools(model: BaseChatModel, tools: Array<DynamicStructuredTool | Tool>) {
	const modelTools = (model.metadata?.tools as Tool[]) ?? [];
	const allTools = [...tools, ...modelTools];
	return allTools;
}

/**
 * Creates an agent executor with the given configuration
 */
function createAgentSequence(
	model: BaseChatModel,
	tools: Array<DynamicStructuredTool | Tool>,
	prompt: ChatPromptTemplate,
	_options: { maxIterations?: number; returnIntermediateSteps?: boolean },
	outputParser?: N8nOutputParser,
	memory?: BaseChatMemory,
	fallbackModel?: BaseChatModel | null,
) {
	const agent = createToolCallingAgent({
		llm: model,
		tools: getAllTools(model, tools),
		prompt,
		streamRunnable: false,
	});

	let fallbackAgent: AgentRunnableSequence | undefined;
	if (fallbackModel) {
		fallbackAgent = createToolCallingAgent({
			llm: fallbackModel,
			tools: getAllTools(fallbackModel, tools),
			prompt,
			streamRunnable: false,
		});
	}
	const runnableAgent = RunnableSequence.from([
		fallbackAgent ? agent.withFallbacks([fallbackAgent]) : agent,
		getAgentStepsParser(outputParser, memory),
		fixEmptyContentMessage,
	]) as AgentRunnableSequence;

	runnableAgent.singleAction = true;
	runnableAgent.streamRunnable = false;

	return runnableAgent;
}

type IntermediateStep = {
	action: {
		tool: string;
		toolInput: Record<string, unknown>;
		log: string;
		messageLog: unknown[];
		toolCallId: string;
		type: string;
	};
	observation?: string;
};

type AgentResult = {
	output: string;
	intermediateSteps?: IntermediateStep[];
	toolCalls?: ToolCallRequest[];
};

async function processEventStream(
	ctx: IExecuteFunctions,
	eventStream: IterableReadableStream<StreamEvent>,
	itemIndex: number,
	returnIntermediateSteps: boolean = false,
	memory?: BaseChatMemory,
	input?: string,
): Promise<AgentResult> {
	const agentResult: AgentResult = {
		output: '',
	};

	if (returnIntermediateSteps) {
		agentResult.intermediateSteps = [];
	}

	const toolCalls: ToolCallRequest[] = [];

	ctx.sendChunk('begin', itemIndex);
	for await (const event of eventStream) {
		// Stream chat model tokens as they come in
		switch (event.event) {
			case 'on_chat_model_stream':
				const chunk = event.data?.chunk as AIMessageChunk;
				if (chunk?.content) {
					const chunkContent = chunk.content;
					let chunkText = '';
					if (Array.isArray(chunkContent)) {
						for (const message of chunkContent) {
							if (message?.type === 'text') {
								chunkText += (message as MessageContentText)?.text;
							}
						}
					} else if (typeof chunkContent === 'string') {
						chunkText = chunkContent;
					}
					ctx.sendChunk('item', itemIndex, chunkText);

					agentResult.output += chunkText;
				}
				break;
			case 'on_chat_model_end':
				// Capture full LLM response with tool calls for intermediate steps
				if (event.data) {
					const chatModelData = event.data as {
						output?: { tool_calls?: ToolCall[]; content?: string };
					};
					const output = chatModelData.output;

					// Check if this LLM response contains tool calls
					if (output?.tool_calls && output.tool_calls.length > 0) {
						// Collect tool calls for request building
						for (const toolCall of output.tool_calls) {
							toolCalls.push({
								tool: toolCall.name,
								toolInput: toolCall.args,
								toolCallId: toolCall.id || 'unknown',
								type: toolCall.type || 'tool_call',
								log:
									output.content ||
									`Calling ${toolCall.name} with input: ${JSON.stringify(toolCall.args)}`,
								messageLog: [output],
							});
						}

						// Also add to intermediate steps if needed
						if (returnIntermediateSteps) {
							for (const toolCall of output.tool_calls) {
								agentResult.intermediateSteps!.push({
									action: {
										tool: toolCall.name,
										toolInput: toolCall.args,
										log:
											output.content ||
											`Calling ${toolCall.name} with input: ${JSON.stringify(toolCall.args)}`,
										messageLog: [output], // Include the full LLM response
										toolCallId: toolCall.id || 'unknown',
										type: toolCall.type || 'tool_call',
									},
								});
							}
						}
					}
				}
				break;
			case 'on_tool_end':
				// Capture tool execution results and match with action
				if (returnIntermediateSteps && event.data && agentResult.intermediateSteps!.length > 0) {
					const toolData = event.data as { output?: string };
					// Find the matching intermediate step for this tool call
					const matchingStep = agentResult.intermediateSteps!.find(
						(step) => !step.observation && step.action.tool === event.name,
					);
					if (matchingStep) {
						matchingStep.observation = toolData.output || '';
					}
				}
				break;
			default:
				break;
		}
	}
	ctx.sendChunk('end', itemIndex);

	// Save conversation to memory if memory is connected
	if (memory && input && agentResult.output) {
		await memory.saveContext({ input }, { output: agentResult.output });
	}

	// Include collected tool calls in the result
	if (toolCalls.length > 0) {
		agentResult.toolCalls = toolCalls;
	}

	return agentResult;
}

export type RequestResponseMetadata = {
	itemIndex?: number;
	previousRequests: ToolCallData[];
	iterationCount?: number;
};

type ToolCallData = {
	action: {
		tool: string;
		toolInput: Record<string, unknown>;
		log: string | number | true | object;
		toolCallId: IDataObject | GenericValue | GenericValue[] | IDataObject[];
		type: string | number | true | object;
	};
	observation: string;
};

function buildSteps(
	response: EngineResponse<RequestResponseMetadata> | undefined,
	itemIndex: number,
): ToolCallData[] {
	const steps: ToolCallData[] = [];

	if (response) {
		const responses = response?.actionResponses ?? [];

		if (response.metadata?.previousRequests) {
			steps.push(...response.metadata.previousRequests);
		}

		for (const tool of responses) {
			if (tool.action?.metadata?.itemIndex !== itemIndex) continue;

			const toolInput: IDataObject = {
				...tool.action.input,
				id: tool.action.id,
			};
			if (!toolInput || !tool.data) {
				continue;
			}

			const step = steps.find((step) => step.action.toolCallId === toolInput.id);
			if (step) {
				continue;
			}
			// Create a synthetic AI message for the messageLog
			// This represents the AI's decision to call the tool
			const syntheticAIMessage = new AIMessage({
				content: `Calling ${tool.action.nodeName} with input: ${JSON.stringify(toolInput)}`,
				tool_calls: [
					{
						id: (toolInput?.id as string) ?? 'reconstructed_call',
						name: nodeNameToToolName(tool.action.nodeName),
						args: toolInput,
						type: 'tool_call',
					},
				],
			});

			const toolResult = {
				action: {
					tool: nodeNameToToolName(tool.action.nodeName),
					toolInput: (toolInput.input as IDataObject) || {},
					log: toolInput.log || syntheticAIMessage.content,
					messageLog: [syntheticAIMessage],
					toolCallId: toolInput?.id,
					type: toolInput.type || 'tool_call',
				},
				observation: JSON.stringify(tool.data?.data?.ai_tool?.[0]?.map((item) => item?.json) ?? ''),
			};

			steps.push(toolResult);
		}
	}
	return steps;
}

/* -----------------------------------------------------------
   Main Executor Function
----------------------------------------------------------- */
/**
 * The main executor method for the Tools Agent.
 *
 * This function retrieves necessary components (model, memory, tools), prepares the prompt,
 * creates the agent, and processes each input item. The error handling for each item is also
 * managed here based on the node's continueOnFail setting.
 *
 * @param this Execute context. SupplyDataContext is passed when agent is as a tool
 *
 * @returns The array of execution data for all processed items
 */
export async function toolsAgentExecute(
	this: IExecuteFunctions | ISupplyDataFunctions,
	response?: EngineResponse<RequestResponseMetadata>,
): Promise<INodeExecutionData[][] | EngineRequest<RequestResponseMetadata>> {
	this.logger.debug('Executing Tools Agent V3 with Langfuse');

	const returnData: INodeExecutionData[] = [];
	let request: EngineRequest<RequestResponseMetadata> | undefined = undefined;

	const items = this.getInputData();
	const batchSize = this.getNodeParameter('options.batching.batchSize', 0, 1) as number;
	const delayBetweenBatches = this.getNodeParameter(
		'options.batching.delayBetweenBatches',
		0,
		0,
	) as number;
	const needsFallback = this.getNodeParameter('needsFallback', 0, false) as boolean;
	const memory = await getOptionalMemory(this);
	const model = await getChatModel(this, 0);
	assert(model, 'Please connect a model to the Chat Model input');
	const fallbackModel = needsFallback ? await getChatModel(this, 1) : null;

	if (needsFallback && !fallbackModel) {
		throw new NodeOperationError(
			this.getNode(),
			'Please connect a model to the Fallback Model input or disable the fallback option',
		);
	}

	for (let i = 0; i < items.length; i += batchSize) {
		const batch = items.slice(i, i + batchSize);
		const batchPromises = batch.map(async (_item, batchItemIndex) => {
			const itemIndex = i + batchItemIndex;

			if (response && response?.metadata?.itemIndex === itemIndex) {
				return null;
			}

			const steps = buildSteps(response, itemIndex);

			const input = getPromptInputByType({
				ctx: this,
				i: itemIndex,
				inputKey: 'text',
				promptTypeKey: 'promptType',
			});
			if (input === undefined) {
				throw new NodeOperationError(this.getNode(), 'The "text" parameter is empty.');
			}
			const outputParser = await getOptionalOutputParser(this, itemIndex);
			const tools = await getTools(this, outputParser);
			const options = this.getNodeParameter('options', itemIndex) as {
				systemMessage?: string;
				maxIterations?: number;
				returnIntermediateSteps?: boolean;
				passthroughBinaryImages?: boolean;
				enableStreaming?: boolean;
				maxTokensFromMemory?: number;
			};

			if (options.enableStreaming === undefined) {
				options.enableStreaming = true;
			}

			// ====== Langfuse Integration Start ======
			this.logger.info('[Langfuse V3] Starting Langfuse integration setup');
			const langfuseCreds = await this.getCredentials('langfuseCustomApi');
			this.logger.info('[Langfuse V3] Credentials retrieved', { baseUrl: langfuseCreds.url });

			const rawMetadata = this.getNodeParameter('langfuseMetadata', itemIndex, {}) as any;
			this.logger.info('[Langfuse V3] Raw metadata from node parameter', { rawMetadata });

			let parsedCustomMetadata: Record<string, unknown> | undefined;
			if (typeof rawMetadata.customMetadata === 'string') {
				try {
					parsedCustomMetadata = JSON.parse(rawMetadata.customMetadata);
					this.logger.info('[Langfuse V3] Parsed customMetadata from string', { parsedCustomMetadata });
				} catch (e) {
					this.logger.warn('[Langfuse V3] Invalid JSON in Langfuse metadata, ignoring customMetadata.', { error: e });
				}
			} else {
				parsedCustomMetadata = rawMetadata.customMetadata;
				this.logger.info('[Langfuse V3] Using customMetadata as-is (not string)', { parsedCustomMetadata });
			}

			// Parse tags from node parameter - support both string (comma-separated) and array formats
			const explicitTags = typeof rawMetadata.tags === 'string'
				? rawMetadata.tags.split(',').map((t: string) => t.trim()).filter(Boolean)
				: (Array.isArray(rawMetadata.tags) ? rawMetadata.tags : []);

			// Also allow inheriting context from the connected Chat Model node
			// The model may already have sessionId/userId/tags set in its metadata
			const modelMetadata = (model as any)?.metadata ?? {};
			const modelSessionId = (modelMetadata.sessionId ?? modelMetadata['session.id']) as string | undefined;
			const modelUserId = (modelMetadata.userId ?? modelMetadata['user.id']) as string | undefined;
			const modelTagsRaw = (modelMetadata.langfuseTags ?? modelMetadata['langfuse.trace.tags'] ?? modelMetadata.tags) as
				| string
				| string[]
				| undefined;
			const modelTags = Array.isArray(modelTagsRaw)
				? modelTagsRaw
				: typeof modelTagsRaw === 'string'
					? modelTagsRaw.split(',').map((t: string) => t.trim()).filter(Boolean)
					: [];

			const tags = explicitTags.length > 0 ? explicitTags : modelTags;
			this.logger.info('[Langfuse V3] Parsed tags', { explicitTags, modelTags, tags });

			// Node parameters win over model metadata; fall back to model metadata when undefined
			const langfuseMetadata = {
				customMetadata: parsedCustomMetadata,
				sessionId: (rawMetadata.sessionId as string | undefined) ?? modelSessionId,
				userId: (rawMetadata.userId as string | undefined) ?? modelUserId,
				tags,
			};
			this.logger.info('[Langfuse V3] Resolved Langfuse metadata', { langfuseMetadata, modelMetadata });

			// Extract langfusePrompt from customMetadata if present
			const langfusePromptMetadata = parsedCustomMetadata?.langfusePrompt as Record<string, unknown> | undefined;
			this.logger.info('[Langfuse V3] Extracted langfusePrompt metadata', { 
				hasLangfusePrompt: !!langfusePromptMetadata,
				langfusePromptMetadata 
			});

			// CRITICAL: Manually fetch prompt from Langfuse for linking
			let fetchedPrompt: any = null;
			if (langfusePromptMetadata) {
				try {
					const langfuseClient = new Langfuse({
						publicKey: langfuseCreds.publicKey as string,
						secretKey: langfuseCreds.secretKey as string,
						baseUrl: (langfuseCreds.url as string) ?? process.env.LANGFUSE_HOST,
					});

					const promptName = langfusePromptMetadata.name as string;
					const promptVersion = langfusePromptMetadata.version as number | undefined;

					if (promptName) {
						if (promptVersion) {
							fetchedPrompt = await langfuseClient.getPrompt(promptName, promptVersion);
						} else {
							fetchedPrompt = await langfuseClient.getPrompt(promptName);
						}
						this.logger.info('[Langfuse V3] Fetched prompt from Langfuse', {
							promptName,
							promptVersion,
							fetched: !!fetchedPrompt,
						});
					}
				} catch (error) {
					this.logger.error('[Langfuse V3] Failed to fetch prompt from Langfuse', { error });
				}
			}

			// Include fetched prompt in metadata if available
			const handlerMetadata = {
				...langfuseMetadata.customMetadata,
				...(fetchedPrompt ? { prompt: fetchedPrompt } : {}),
			};

			const baseLangfuseHandler = new CallbackHandler({
				publicKey: langfuseCreds.publicKey as string,
				secretKey: langfuseCreds.secretKey as string,
				baseUrl: (langfuseCreds.url as string) ?? process.env.LANGFUSE_HOST,
				sessionId: langfuseMetadata.sessionId,
				userId: langfuseMetadata.userId,
				metadata: handlerMetadata,
				...(fetchedPrompt ? { prompt: fetchedPrompt } : {}),
			});

			// Wrap handler to log callback invocations
			const logger = this.logger;  // Capture logger reference
			const originalHandleChainStart = baseLangfuseHandler.handleChainStart?.bind(baseLangfuseHandler);
			const originalHandleChainEnd = baseLangfuseHandler.handleChainEnd?.bind(baseLangfuseHandler);
			const originalHandleLLMStart = baseLangfuseHandler.handleLLMStart?.bind(baseLangfuseHandler);
			const originalHandleLLMEnd = baseLangfuseHandler.handleLLMEnd?.bind(baseLangfuseHandler);
			const originalHandleChatModelStart = (baseLangfuseHandler as any).handleChatModelStart?.bind(baseLangfuseHandler);
			const originalHandleToolStart = baseLangfuseHandler.handleToolStart?.bind(baseLangfuseHandler);
			const originalHandleToolEnd = baseLangfuseHandler.handleToolEnd?.bind(baseLangfuseHandler);

			if (originalHandleChainStart) {
				baseLangfuseHandler.handleChainStart = async (...args: any[]) => {
					logger.info('[Langfuse V3] 🔗 handleChainStart invoked', { chain: args[0]?.name });
					return (originalHandleChainStart as any)(...args);
				};
			}
			if (originalHandleChainEnd) {
				baseLangfuseHandler.handleChainEnd = async (...args: any[]) => {
					logger.info('[Langfuse V3] ✅ handleChainEnd invoked');
					return (originalHandleChainEnd as any)(...args);
				};
			}
			if (originalHandleLLMStart) {
				baseLangfuseHandler.handleLLMStart = async (...args: any[]) => {
					logger.info('[Langfuse V3] 🤖 handleLLMStart invoked', { llm: args[0]?.name });
					return (originalHandleLLMStart as any)(...args);
				};
			}
			if (originalHandleLLMEnd) {
				baseLangfuseHandler.handleLLMEnd = async (...args: any[]) => {
					logger.info('[Langfuse V3] ✅ handleLLMEnd invoked', { tokenUsage: args[0]?.llmOutput?.tokenUsage });
					return (originalHandleLLMEnd as any)(...args);
				};
			}
			if (originalHandleChatModelStart) {
				(baseLangfuseHandler as any).handleChatModelStart = async (...args: any[]) => {
					logger.info('[Langfuse V3] 💬 handleChatModelStart invoked', { 
						model: args[0]?.name,
						hasPromptMetadata: !!langfusePromptMetadata,
						args: args.map((a, i) => ({ index: i, type: typeof a, isObject: typeof a === 'object' }))
					});
					// CRITICAL: Inject langfusePrompt into metadata arg for generation-level prompt linking
					// Signature: handleLLMStart(llm, prompts, runId, parentRunId?, extraParams?, tags?, metadata?, runName?)
					// metadata is at args[6]
					if (langfusePromptMetadata) {
						if (!args[6]) {
							args[6] = {}; // Create metadata object if it doesn't exist
						}
						if (typeof args[6] === 'object') {
							args[6].langfusePrompt = langfusePromptMetadata;
							logger.info('[Langfuse V3] Injected langfusePrompt into generation metadata at args[6]', { 
								langfusePrompt: langfusePromptMetadata 
							});
						}
					}
					return (originalHandleChatModelStart as any)(...args);
				};
			}
			if (originalHandleToolStart) {
				baseLangfuseHandler.handleToolStart = async (...args: any[]) => {
					logger.info('[Langfuse V3] 🔧 handleToolStart invoked', { tool: args[0]?.name });
					return (originalHandleToolStart as any)(...args);
				};
			}
			if (originalHandleToolEnd) {
				baseLangfuseHandler.handleToolEnd = async (...args: any[]) => {
					logger.info('[Langfuse V3] ✅ handleToolEnd invoked');
					return (originalHandleToolEnd as any)(...args);
				};
			}

			const langfuseHandler = baseLangfuseHandler;
			this.logger.info('[Langfuse V3] CallbackHandler created and wrapped with debug logging');
			// ====== Langfuse Integration End ======

			// Prepare the prompt messages and prompt template.
			const messages = await prepareMessages(this, itemIndex, {
				systemMessage: options.systemMessage,
				passthroughBinaryImages: options.passthroughBinaryImages ?? true,
				outputParser,
			});
			const prompt: ChatPromptTemplate = preparePrompt(messages);

			// Create executors for primary and fallback models
			const baseExecutor = createAgentSequence(
				model,
				tools,
				prompt,
				options,
				outputParser,
				memory,
				fallbackModel,
			);

			// Bind Langfuse handler to the executor using withConfig()
			// This ensures callbacks propagate to all nested calls
			// CRITICAL: Use langfuseSessionId, langfuseUserId (camelCase) for Langfuse v3/v4
			const executor = baseExecutor.withConfig({
				callbacks: [langfuseHandler],
				metadata: {
					langfuseSessionId: langfuseMetadata.sessionId,
					langfuseUserId: langfuseMetadata.userId,
					langfuseTags: langfuseMetadata.tags,
					...langfuseMetadata.customMetadata,
					// Add prompt metadata if present for prompt linking
					...(langfusePromptMetadata ? { langfusePrompt: langfusePromptMetadata } : {}),
				},
			});
			this.logger.info('[Langfuse V3] Executor configured with Langfuse handler via withConfig');
			// Invoke with fallback logic
			const invokeParams = {
				steps,
				input,
				system_message: options.systemMessage ?? SYSTEM_MESSAGE,
				formatting_instructions:
					'IMPORTANT: For your response to user, you MUST use the `format_final_json_response` tool with your complete answer formatted according to the required schema. Do not attempt to format the JSON manually - always use this tool. Your response will be rejected if it is not properly formatted through this tool. Only use this tool once you are ready to provide your final answer.',
			};
			const executeOptions = {
				signal: this.getExecutionCancelSignal(),
			};
			this.logger.info('[Langfuse V3] Execute options prepared (callbacks bound via withConfig)');

			// Check if streaming is actually available
			const isStreamingAvailable = 'isStreaming' in this ? this.isStreaming?.() : undefined;

			if (
				'isStreaming' in this &&
				options.enableStreaming &&
				isStreamingAvailable &&
				this.getNode().typeVersion >= 2.1
			) {
				let chatHistory: BaseMessage[] | undefined = undefined;
				if (memory) {
					// Load memory variables to respect context window length
					chatHistory = await loadChatHistory(memory, model, options.maxTokensFromMemory);
				}
				// Wrap in propagateAttributes to set OTEL trace-level attributes for Langfuse session tracking
				this.logger.info('[Langfuse V3] Wrapping streaming execution with OTEL propagateAttributes', {
					sessionId: langfuseMetadata.sessionId,
					userId: langfuseMetadata.userId,
					tags: langfuseMetadata.tags,
				});
			// Convert metadata to Record<string, string> as required by OTEL
			// Special handling: extract langfusePrompt fields separately for prompt linking
			const otelMetadata: Record<string, string> = {};
			if (langfuseMetadata.customMetadata) {
				for (const [key, value] of Object.entries(langfuseMetadata.customMetadata)) {
					if (key === 'langfusePrompt') {
						// Flatten langfusePrompt for OTEL: { name, version, config } -> separate keys
						const promptObj = value as Record<string, unknown>;
						if (promptObj.name) otelMetadata['langfuse.prompt.name'] = String(promptObj.name);
						if (promptObj.version) otelMetadata['langfuse.prompt.version'] = String(promptObj.version);
						if (promptObj.config) otelMetadata['langfuse.prompt.config'] = JSON.stringify(promptObj.config);
					} else {
						otelMetadata[key] = typeof value === 'string' ? value : JSON.stringify(value);
					}
				}
			}
			const result = await propagateAttributes(
				{
					sessionId: langfuseMetadata.sessionId,
					userId: langfuseMetadata.userId,
					metadata: Object.keys(otelMetadata).length > 0 ? otelMetadata : undefined,
					...(langfuseMetadata.tags && langfuseMetadata.tags.length > 0
						? { tags: langfuseMetadata.tags }
						: {}),
				},
					async () => {
						const eventStream = executor.streamEvents(
							{
								...invokeParams,
								chat_history: chatHistory,
							},
							{
								version: 'v2',
								...executeOptions,
							},
						);

						this.logger.info('[Langfuse V3] Starting streaming execution');
						return await processEventStream(
							this,
							eventStream,
							itemIndex,
							options.returnIntermediateSteps,
							memory,
							input,
						);
					},
				);
				this.logger.info('[Langfuse V3] Streaming execution completed');

				// Flush Langfuse handler to ensure traces are sent
				this.logger.info('[Langfuse V3] Flushing Langfuse handler...');
				try {
					await langfuseHandler.flushAsync();
					this.logger.info('[Langfuse V3] Langfuse handler flushed successfully');
				} catch (flushError) {
					this.logger.error('[Langfuse V3] Error flushing Langfuse handler', { error: flushError });
				}

				// If result contains tool calls, build the request object like the normal flow
				if (result.toolCalls && result.toolCalls.length > 0) {
					const currentIteration = (response?.metadata?.iterationCount ?? 0) + 1;

					// Check if we've exceeded maxIterations
					if (options.maxIterations && currentIteration > options.maxIterations) {
						throw new NodeOperationError(this.getNode(), 'Maximum iterations reached');
					}

					const actions = await createEngineRequests(result.toolCalls, itemIndex, tools);

					return {
						actions,
						metadata: {
							previousRequests: buildSteps(response, itemIndex),
							iterationCount: currentIteration,
						},
					};
				}

				return result;
			} else {
				// Handle regular execution
				let chatHistory: BaseMessage[] | undefined = undefined;
				if (memory) {
					// Load memory variables to respect context window length
					chatHistory = await loadChatHistory(memory, model, options.maxTokensFromMemory);
				}
				// Wrap in propagateAttributes to set OTEL trace-level attributes for Langfuse session tracking
				this.logger.info('[Langfuse V3] Wrapping non-streaming execution with OTEL propagateAttributes', {
					sessionId: langfuseMetadata.sessionId,
					userId: langfuseMetadata.userId,
					tags: langfuseMetadata.tags,
				});
			// Convert metadata to Record<string, string> as required by OTEL
			// Special handling: extract langfusePrompt fields separately for prompt linking
			const otelMetadata: Record<string, string> = {};
			if (langfuseMetadata.customMetadata) {
				for (const [key, value] of Object.entries(langfuseMetadata.customMetadata)) {
					if (key === 'langfusePrompt') {
						// Flatten langfusePrompt for OTEL: { name, version, config } -> separate keys
						const promptObj = value as Record<string, unknown>;
						if (promptObj.name) otelMetadata['langfuse.prompt.name'] = String(promptObj.name);
						if (promptObj.version) otelMetadata['langfuse.prompt.version'] = String(promptObj.version);
						if (promptObj.config) otelMetadata['langfuse.prompt.config'] = JSON.stringify(promptObj.config);
					} else {
						otelMetadata[key] = typeof value === 'string' ? value : JSON.stringify(value);
					}
				}
			}
			const modelResponse = await propagateAttributes(
				{
					sessionId: langfuseMetadata.sessionId,
					userId: langfuseMetadata.userId,
					metadata: Object.keys(otelMetadata).length > 0 ? otelMetadata : undefined,
					...(langfuseMetadata.tags && langfuseMetadata.tags.length > 0
						? { tags: langfuseMetadata.tags }
						: {}),
				},
					async () => {
						this.logger.info('[Langfuse V3] Starting non-streaming execution');
						return await executor.invoke(
							{
								...invokeParams,
								chat_history: chatHistory,
							},
							executeOptions,
						);
					},
				);
				this.logger.info('[Langfuse V3] Non-streaming execution completed');

				// Flush Langfuse handler to ensure traces are sent
				this.logger.info('[Langfuse V3] Flushing Langfuse handler...');
				try {
					await langfuseHandler.flushAsync();
					this.logger.info('[Langfuse V3] Langfuse handler flushed successfully');
				} catch (flushError) {
					this.logger.error('[Langfuse V3] Error flushing Langfuse handler', { error: flushError });
				}

				if ('returnValues' in modelResponse) {
					// Save conversation to memory including any tool call context
					if (memory && input && modelResponse.returnValues.output) {
						// If there were tool calls in this conversation, include them in the context
						let fullOutput = modelResponse.returnValues.output as string;

						if (steps.length > 0) {
							// Include tool call information in the conversation context
							const toolContext = steps
								.map(
									(step) =>
										`Tool: ${step.action.tool}, Input: ${JSON.stringify(step.action.toolInput)}, Result: ${step.observation}`,
								)
								.join('; ');
							fullOutput = `[Used tools: ${toolContext}] ${fullOutput}`;
						}

						await memory.saveContext({ input }, { output: fullOutput });
					}
					// Include intermediate steps if requested
					const result = { ...modelResponse.returnValues };
					if (options.returnIntermediateSteps && steps.length > 0) {
						result.intermediateSteps = steps;
					}
					return result;
				}

				const currentIteration = (response?.metadata?.iterationCount ?? 0) + 1;

				// Check if we've exceeded maxIterations
				if (options.maxIterations && currentIteration > options.maxIterations) {
					throw new NodeOperationError(this.getNode(), 'Maximum iterations reached');
				}

				const actions = await createEngineRequests(modelResponse, itemIndex, tools);

				return {
					actions,
					metadata: {
						previousRequests: buildSteps(response, itemIndex),
						iterationCount: currentIteration,
					},
				};
			}
		});

		const batchResults = await Promise.allSettled(batchPromises);
		// This is only used to check if the output parser is connected
		// so we can parse the output if needed. Actual output parsing is done in the loop above
		const outputParser = await getOptionalOutputParser(this, 0);
		batchResults.forEach((result, index) => {
			const itemIndex = i + index;
			if (result.status === 'rejected') {
				const error = result.reason as Error;
				if (this.continueOnFail()) {
					returnData.push({
						json: { error: error.message },
						pairedItem: { item: itemIndex },
					} as INodeExecutionData);
					return;
				} else {
					throw new NodeOperationError(this.getNode(), error);
				}
			}
			const response = result.value;

			if ('actions' in response) {
				if (!request) {
					request = {
						actions: response.actions,
						metadata: response.metadata,
					};
				} else {
					request.actions.push(...response.actions);
				}
				return;
			}

			// If memory and outputParser are connected, parse the output.
			if (memory && outputParser) {
				const parsedOutput = jsonParse<{ output: Record<string, unknown> }>(
					response.output as string,
				);
				response.output = parsedOutput?.output ?? parsedOutput;
			}

			// Omit internal keys before returning the result.
			const itemResult: INodeExecutionData = {
				json: omit(
					response,
					'system_message',
					'formatting_instructions',
					'input',
					'chat_history',
					'agent_scratchpad',
				),
				pairedItem: { item: itemIndex },
			};

			returnData.push(itemResult);
		});

		if (i + batchSize < items.length && delayBetweenBatches > 0) {
			await sleep(delayBetweenBatches);
		}
	}
	// Check if we have any Request objects (tool calls)
	if (request) {
		return request;
	}

	// Otherwise return execution data
	return [returnData];
}
async function loadChatHistory(
	memory: BaseChatMemory,
	model: BaseChatModel,
	maxTokensFromMemory?: number,
): Promise<BaseMessage[]> {
	const memoryVariables = await memory.loadMemoryVariables({});
	let chatHistory = memoryVariables['chat_history'] as BaseMessage[];

	if (maxTokensFromMemory) {
		chatHistory = await trimMessages(chatHistory, {
			strategy: 'last',
			maxTokens: maxTokensFromMemory,
			tokenCounter: model,
			includeSystem: true,
			startOn: 'human',
			allowPartial: true,
		});
	}

	return chatHistory;
}
