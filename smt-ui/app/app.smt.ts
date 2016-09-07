import { NgModule }      from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { Navigation }    from './app.navigation';
import { CurrentUser }   from './app.cur.user';
import { Profiles }      from './page/app.profiles';
import { HttpModule }    from "@angular/http";

@NgModule({
    imports:      [ BrowserModule, HttpModule ],
    declarations: [ Navigation, CurrentUser, Profiles ],
    bootstrap:    [ Navigation, Profiles ]
})
export class SMT { }