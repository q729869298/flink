/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.codesplit;

import org.apache.flink.annotation.Internal;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.atn.PredictionMode;

import java.util.LinkedHashSet;

/** If rewriter. Rewrite method to two part: 1.if true method. 2.if false method. */
@Internal
public class IfStatementRewriter {

    private final long maxMethodLength;
    private IfStatementVisitor visitor;

    public IfStatementRewriter(String code, long maxMethodLength) {
        this.maxMethodLength = maxMethodLength;
        this.visitor = new IfStatementVisitor(code);
    }

    public String rewrite() {
        String rewriterCode = visitor.rewriteAndGetCode();
        while (visitor.hasRewrite()) {
            visitor = new IfStatementVisitor(rewriterCode);
            rewriterCode = visitor.rewriteAndGetCode();
        }
        return rewriterCode;
    }

    private class IfStatementVisitor extends JavaParserBaseVisitor<Void> {

        private final CommonTokenStream tokenStream;

        private final TokenStreamRewriter rewriter;

        private long rewriteCount;

        private IfStatementVisitor(String code) {
            this.tokenStream = new CommonTokenStream(new JavaLexer(CharStreams.fromString(code)));
            this.rewriter = new TokenStreamRewriter(tokenStream);
        }

        @Override
        public Void visitMethodDeclaration(JavaParser.MethodDeclarationContext ctx) {

            if (!"void".equals(ctx.typeTypeOrVoid().getText())) {
                return null;
            }

            // function real parameters
            LinkedHashSet<String> declarationContext = new LinkedHashSet<>();
            new JavaParserBaseVisitor<Void>() {
                @Override
                public Void visitFormalParameter(JavaParser.FormalParameterContext ctx) {
                    declarationContext.add(ctx.variableDeclaratorId().getText());
                    return null;
                }
            }.visit(ctx);

            String type = CodeSplitUtil.getContextString(ctx.typeTypeOrVoid());
            String functionName = ctx.IDENTIFIER().getText();
            String parameters = CodeSplitUtil.getContextString(ctx.formalParameters());

            String methodQualifier = "";
            if (ctx.THROWS() != null) {
                methodQualifier =
                        " throws " + CodeSplitUtil.getContextString(ctx.qualifiedNameList());
            }

            for (JavaParser.BlockStatementContext blockStatementContext :
                    ctx.methodBody().block().blockStatement()) {

                if (blockStatementContext.statement() != null
                        && blockStatementContext.statement().IF() != null
                        && blockStatementContext.statement().getText().length() > maxMethodLength) {
                    if (blockStatementContext.statement().statement(0) != null
                            && blockStatementContext.statement().statement(0).block() != null
                            && blockStatementContext
                                            .statement()
                                            .statement(0)
                                            .block()
                                            .blockStatement()
                                    != null
                            && blockStatementContext
                                            .statement()
                                            .statement(0)
                                            .block()
                                            .blockStatement()
                                            .size()
                                    > 1) {

                        long counter = CodeSplitUtil.getCounter().incrementAndGet();

                        String methodDef =
                                type
                                        + " "
                                        + functionName
                                        + "_trueFilter"
                                        + counter
                                        + parameters
                                        + methodQualifier;

                        String newMethod =
                                methodDef
                                        + CodeSplitUtil.getContextString(
                                                blockStatementContext
                                                        .statement()
                                                        .statement(0)
                                                        .block())
                                        + "\n";

                        String newMethodCall =
                                functionName
                                        + "_trueFilter"
                                        + counter
                                        + "("
                                        + String.join(", ", declarationContext)
                                        + ");\n";
                        rewriter.replace(
                                blockStatementContext.statement().statement(0).block().start,
                                blockStatementContext.statement().statement(0).block().stop,
                                "{\n" + newMethodCall + "\n}\n");
                        rewriter.insertAfter(ctx.getParent().stop, "\n" + newMethod + "\n");
                        rewriteCount++;
                    }

                    if (blockStatementContext.statement().statement(1) != null
                            && blockStatementContext.statement().statement(1).block() != null
                            && blockStatementContext
                                            .statement()
                                            .statement(1)
                                            .block()
                                            .blockStatement()
                                    != null
                            && blockStatementContext
                                            .statement()
                                            .statement(1)
                                            .block()
                                            .blockStatement()
                                            .size()
                                    > 1) {
                        long counter = CodeSplitUtil.getCounter().incrementAndGet();

                        String methodDef =
                                type
                                        + " "
                                        + functionName
                                        + "_falseFilter"
                                        + counter
                                        + parameters
                                        + methodQualifier;

                        String newMethod =
                                methodDef
                                        + CodeSplitUtil.getContextString(
                                                blockStatementContext
                                                        .statement()
                                                        .statement(1)
                                                        .block())
                                        + "\n";

                        String newMethodCall =
                                functionName
                                        + "_falseFilter"
                                        + counter
                                        + "("
                                        + String.join(", ", declarationContext)
                                        + ");\n";
                        rewriter.replace(
                                blockStatementContext.statement().statement(1).block().start,
                                blockStatementContext.statement().statement(1).block().stop,
                                "{\n" + newMethodCall + "\n}\n");
                        rewriter.insertAfter(ctx.getParent().stop, "\n" + newMethod + "\n");
                        rewriteCount++;
                    }
                }
            }
            return null;
        }

        private String rewriteAndGetCode() {
            JavaParser javaParser = new JavaParser(tokenStream);
            javaParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            visit(javaParser.compilationUnit());
            return rewriter.getText();
        }

        private boolean hasRewrite() {
            return rewriteCount > 0L;
        }
    }
}
