/** 
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
* 
*   http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

/**
 * Function to synchronize all tabs on a page. 
 * 
 * See layouts/shortcodes/tabs.html
 */
function onSwitch(tabId) {
    allTabItems = document.querySelectorAll("[data-tab-group='flink-tabs']");
    targetTabItems = document.querySelectorAll("[data-tab-group='flink-tabs'][data-tab-item='" + tabId + "']");

    allTabItems.forEach(function(tab) {
        tab.removeAttribute("checked")
    })

    targetTabItems.forEach(function(tab) {
        tab.setAttribute("checked", "checked")
    })
}

/**
 * Function to collapse the ToC in desktop mode.
 */
function collapseToc() {
    document.querySelector(".book-toc").setAttribute("style", "display:none");
    document.querySelector(".expand-toc").setAttribute("style", "display:block");

    sessionStorage.setItem("collapse-toc", "true");
}

/**
 * Function to expand the ToC in desktop mode.
 */
function expandToc() {
    document.querySelector(".book-toc").setAttribute("style", "display:block");
    document.querySelector(".expand-toc").setAttribute("style", "display:none");
    sessionStorage.removeItem("collapse-toc");
}

/**
 * Selects all text within the given container.
 */
function selectText(containerId) {
    if (document.selection) {
        var range = document.body.createTextRange();
        range.moveToElementText(document.getElementById(containerId));
        range.select().createTextRange();
        document.execCommand("copy");
    } else if (window.getSelection) {
        var range = document.createRange();
        range.selectNode(document.getElementById(containerId));
        window.getSelection().removeAllRanges();
        window.getSelection().addRange(range);
        document.execCommand("copy");
    }

    document.querySelectorAll("[copyable='flink-module']").forEach(function(alert) {
        alert.setAttribute("style", "text-align:center;display:none");
    });

    document.querySelectorAll("[copyable='flink-module'][copyattribute='" + containerId +"'").forEach(function(alert) {
        alert.setAttribute("style", "text-align:center;display:block");
    });
}

document.addEventListener("DOMContentLoaded", function(event) { 
    if (sessionStorage.getItem("collapse-toc") === "true") {
        collapseToc();
    }

    // Display anchor links when hovering over headers. For documentation of the
    // configuration options, see the AnchorJS documentation.
    anchors.options = {
        placement: 'right'
    };
    anchors.add();
});