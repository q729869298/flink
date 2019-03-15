/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/// <reference path="../../../../../node_modules/monaco-editor/monaco.d.ts" />

import { AfterViewInit, Component, ElementRef, ChangeDetectionStrategy, Input, OnDestroy } from '@angular/core';
import IStandaloneCodeEditor = monaco.editor.IStandaloneCodeEditor;

@Component({
  selector       : 'flink-monaco-editor',
  template       : ``,
  styleUrls      : [ './monaco-editor.component.less' ],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class MonacoEditorComponent implements AfterViewInit, OnDestroy {
  private innerValue = '';
  private editor: IStandaloneCodeEditor;

  @Input()
  set value(value) {
    this.innerValue = value;
    if (this.editor) {
      this.editor.getModel()!.setValue(this.innerValue);
    }
  }

  get value() {
    return this.innerValue;
  }

  setupMonaco() {
    const hostElement = this.elementRef.nativeElement;
    this.editor = monaco.editor.create(hostElement, {
      scrollBeyondLastLine: false,
      glyphMargin         : true,
      language            : 'apex',
      wordWrap            : 'on',
      readOnly            : true,
      minimap             : {
        enabled: false
      }
    });
    if (this.value) {
      this.editor.getModel()!.setValue(this.value);
    }
  }

  layout() {
    if (this.editor) {
      this.editor.layout();
    }
  }

  constructor(private elementRef: ElementRef) {
  }

  ngAfterViewInit() {
    const windowAny = window as any;
    if (windowAny.monaco) {
      this.setupMonaco();
    } else {
      const script = document.createElement('script');
      script.type = 'text/javascript';
      script.src = 'libs/vs/loader.js';
      script.onload = () => {
        const onGotAmdLoader = () => {
          // Load monaco
          windowAny.require.config({ paths: { vs: 'libs/vs' } });
          windowAny.require([ 'vs/editor/editor.main' ], () => {
            setTimeout(() => {
              this.setupMonaco();
            });
          });
        };
        onGotAmdLoader();
      };
      // Add the script tag to the page in order to start loading monaco
      document.body.appendChild(script);
    }
  }

  ngOnDestroy() {
    this.editor.dispose();
  }

}
