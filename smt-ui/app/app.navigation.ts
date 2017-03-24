import {Component, Directive, HostListener, Host, Input, Output, EventEmitter} from '@angular/core';

@Directive({
    selector: '[tab]'
})
export class TabBarElement {
    @Input('tab') tabName: string;
    @Output() change = new EventEmitter<string>();
    numberOfClicks = 0;

    @HostListener('click', ['$event.target'])
    onClick() {
        this.change.emit(this.tabName);
    }

    constructor() { }
}

@Component({
    selector: 'smt-navigation',
    templateUrl: '../templates/navigation.html'
})
export class Navigation {
    public currentTab : string;
    private tabsDict = {
        //'profiles': "Profiles",
        'annotate': "Annotate"
    };

    tabs() : Array<string> {
        return Object.keys(this.tabsDict);
    }

    constructor() { this.currentTab = 'annotate' }

    isCurTab(tab : string) {
        return tab == this.currentTab;
    }

    changeTab(tab : string) {
        this.currentTab = tab;
    }
}
