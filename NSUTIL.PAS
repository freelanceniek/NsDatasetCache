unit NSUtil;
{:********************************************************************
*  NSUtil  <br/>
*  all System  <br/>
*  Date 01.10.2017 <br/>
*  Copyright 2010 by MicroObjects, Licence given to Ergomed Plc  <br/>
*  @author Niek Sluijter <br/>
*  @Desc  collection of Helper Interfaces, Classes for various areas
**********************************************************************}
{
  NS basis functions and objects (Subset of AAUtil)
  -Base classes
  -Logging/Message interface voor componenten (make it abstract)
}
interface

uses SysUtils, Classes, TypInfo, Db, Contnrs, IniFiles; // DbTables

const // Message Constanten

    SM_OK      = 1; // msg + ok button
    SM_INVERT  = 2; // knoppen omdraaien  // NS 2010-06 nieuw!
    SM_YESNO   = 4; // msg + yes/no buttons
    SM_OKERR   = 8; // msg + uitroepteken + ok button
    SM_SILENT  = 16; // msg loggen
    SM_INFO    = 16; // msg loggen + tonen (zonder msgbox)
    SM_ABORT   = 32; // msg + silent exception
    SM_ABORTHARD= 64; // hard exception
    SM_OPTABORT= 128; // msg + user kan een exception genereren
    SM_LOG     = 16;
    SM_NOYES   = SM_YESNO+SM_INVERT;
    SM_OKI     = SM_OK+SM_INVERT;

  tkSimpleProperties = [tkInteger, tkChar, tkEnumeration, tkFloat, tkString, tkSet,
                        tkWChar, tkLString, tkWString, tkVariant, tkInt64];

type
  ArrayOfString = array of string;
  ArrayOfConst  = array of TVarRec;
  TCharSet = set of char;


var
  G_SyncGlobalObjects : TMultiReadExclusiveWriteSynchronizer;
  // G_DebugLevel    : Integer;                                                       // SM_XXXX bepaalt wat er alleem gelog word!
  // G_AllowLocalMsg : boolean;
  G_DataPath: string;                                                       // for inifile, logging
  G_AppName:  string;                                                       // current exe or dll
  G_App:      TComponent;                                                   // default owner for components

  G_OnProcessMessages: TNotifyEvent;                                        // In case of Forms app: Application.ProcessMessages

type
  // Iterator interface
  IAAListIterator = interface
    function CurrentObject: TObject;
    procedure DeleteCurrent;
    function Loop: Boolean;
  end;


  // Abstract interface for Forms Application
  function ShowMsg( Sender: TObject; Msg: String; Code : Byte = 1; BtnInverse: boolean = False ): Boolean; overload;
  function ShowMsg( Sender: TObject; Msg: String; Params : Array of Const; Code : Byte = 1 ): Boolean; overload;
  procedure LogError(Sender:TObject; Msg:string);
  procedure Process_Messages;
  function isTerminated: boolean;

type
  TComponentClass = class of TComponent;

type
  IAAStrings = interface ['{751E4EA6-972D-4B40-9CFD-4E179AF929C2}']             // tbv autom mem management
    function GetLoopObject: TObject;
    procedure SetLoopObject(value:TObject);
    //
    function Add(const S: string): Integer;
    function AddObject(const S: string; AObject: TObject): Integer;
    procedure Clear;
    procedure Delete(Index: Integer);
    procedure Exchange(Index1, Index2: Integer);
    function Find(const S: string; var Index: Integer): Boolean;
    function IndexOf(const S: string): Integer;
    function GetCount: Integer;
    procedure SetCount(const value:integer);
    function Get( Index:Integer):String;
    procedure Put( Index:Integer; const Value:String );
    function GetValue(const Name: string): string;
    procedure SetValue(const Name, Value: string);
    function Get_Text: string;  // GetText in XE2 gives problem ?!
    procedure Set_Text(const value:string );
    //
    function obj: TStringList;
    function Complement(aNameOrValue:string): string;
    function Loop(LoopForward:boolean=True): boolean;
    function LoopIndex: Integer;
    function LoopValue: string;
    procedure Reset(LoopForward:boolean=True;aIndex:Integer=-1);
    // property LoopIndex: Integer read GetLoopIndex write SetLoopIndex;

    property Count: Integer read GetCount write SetCount;
    property Strings[Index: Integer]: string read Get write Put; default;
    property Text: string read Get_Text write Set_Text;

    property LoopObject: TObject read GetLoopObject write SetLoopObject;
    property Values[const Name: string]: string read GetValue write SetValue;
  end;
  INStrings = IAAStrings;

  IAAIni = interface ['{FF85F3DE-5272-49DC-9C6A-9D0266D8D2FC}']
    function obj: TIniFile;
    function ReadString(section,name:string):string;
  end;

  TAAStringList = class ( TStringList )
    function Find(const S: string; var Index: Integer): Boolean; override;
  end;

  TStringsIntfImpl = class ( TInterfacedObject, IAAStrings )
  private
    FRestart: boolean;
    FReset: boolean;
    FIndex: integer;
    FList: TStringList;                      // zou naar een functie kunnen
  // tbv lazy mem management
    function GetLoopObject: TObject;
    procedure SetLoopObject(value:TObject);
    function GetValue(const Name: string): string;
    procedure SetValue(const Name, Value: string);
  public
    constructor create(aStringList:TStringlist=nil);
    destructor destroy; override;
    function CompareStrings(const S1, S2: string): Integer;
    function Complement(aNameOrValue:string): string;
    procedure SetCount(const value:integer);
    function obj: TStringList;
    function Loop(LoopForward:boolean=True): boolean;
    function LoopIndex: Integer;
    function LoopValue: string;
    function Get_Text:string;
    procedure Set_Text(const value:string );
    procedure Reset(LoopForward:boolean=True;aIndex:Integer=-1);
    procedure ParseCsv( FieldList: string; var P: Integer; SepChar: Char; EscapeChar: Char = ' '; QuoteChars:string='"''' );
    property SL: TStringList read FList implements IAAStrings;      // tbv interface
  end;

  TIniIntfImpl = class (TInterfacedObject)

  end;

  PMethod = ^TMethod;
  PNotifyEvent = ^TNotifyEvent;

  TEventListIdx = class(TObject)  // op naam geindexeerd
  private
    EventList: TStringList;

    function GetEventCount:integer;
  protected
  public
    SilentOnEventNotFound: boolean;
    //SilentOnException: boolean; -> zie Fire
    constructor Create(aSilentOnEventNotFound: boolean);
    destructor Destroy;override;
    procedure AddNotifyEvent( aName:string; ne: TNotifyEvent );
    procedure DeleteNotifyEvent( aName:string );
    procedure Fire( aName: string; Sender: TObject = nil; SilentOnException: boolean = False );
    property EventCount: integer read GetEventCount;
  end;

  TListHack = class(TObject)
  private
    FList: PPointerList;
    FCount: Integer;
  end;

  TProgressEvent = procedure (Sender:TObject;Position:Integer;Total:Integer) of object;          // tbv progress bar
  TProcessInfoEvent = procedure (Sender:TObject;var aSubject:string; aMessage:String) of object; // voortgangs info


  //function DocFactory: TAADocFactory;
  TAAIniFile = class( TIniFile )
    procedure ReadSectionValues(const Section: String; Strings: TStrings;NewNameValueSeperator: string; RemoveKey: boolean ); overload;
  end;

  // Func voor intern gebruik
  function GetObjectDescription( Sender: TObject ): string;
  //function G_AppIniFile: TAAIniFile;


  function INStringsCreate(UseStringList: TStringList=nil): TStringsIntfImpl;
  function Copy1( S: String; Index: Integer; DefaultCar: Char =' ' ): Char;
  function CopyProps( Src, Dest: TObject; PropNames : array of string ): integer;                                                 // Messages aan local user tonen / loggen
  function CheckSetPropValue( Instance: TObject; PropNames: array of string; Value: Variant ): boolean;
  function CheckPropValue( Instance: TObject; PropNames: array of string; PreferStrings: Boolean = True ): Variant;
  function Explode(s:string;separator:char): ArrayOfString;
  function PropertyObject(Instance:TObject;var PropertyPath:string; AllowObjectPropReturn: Boolean= True ): TObject;
  function iif( Conditie: boolean; Opt1: string; Opt2: string = '' ): string; overload;
  function Iif(Conditie: boolean; Integer1, Integer2: Integer): Integer; overload;
  function Pad(S: string; targetLength: integer; filchar: char = ' '): string;
  function Zero(S: string; targetLength: integer; filchar: char = ' '): string;
  function posv(substr, s: string; vanaf: integer): integer;
  function Escape( const s: string; EscapeChars: string=#0#13#10; EscapeCharsReplace: string='\0rn' ): string;
  function UnEscape( const S:string; EscapeChars: string=#0#13#10; EscapeCharsReplace: string='\0rn' ): string;
  function SepFieldsAdd( const SepField, S: string; SepChar: String; IngoreDupl : boolean ): string;
  function SepFieldsNext( FieldList: string; var P: Integer; SepChar: Char; EscapeChar: Char = ' ' ): string;
  function SepFieldsCount(FieldList: string; SepChar: Char; EscapeChar: Char = ' ' ):integer;
  function SepFieldsLine(const FieldList: string; var P: Integer; aData:TStrings; SepChar: Char; EscapeChar: Char = ' '; Eol:string=#13#10 ): boolean;
  procedure SepFieldsUnEscape( aData:TStrings; EscapeChars:string='"'; EscapeCharsReplace:string='""'; QuoteChars:string='""' );
  function Str2DateTime(const Value: string): TDateTime;
  function SelDir( var strFolder: string; Caption: string = 'Select folder...' ): Boolean;
  function txt2float(const S: string; var aPos: integer; DecimalSep:char='.'; ThousendSep:char='0' ): double;
  function Str2IntDef(const S: string; const def: integer=0 ): integer;
  function Const2Str( const Value: TVarRec; FloatFormat:string='0.##' ): String;
  function Fmt( s:string; params: array of const; QuoteChar:Char='''' ): string;
  function  Files2List(const StartDirAndMask: String; const List: TStrings): Boolean;
  //procedure GetBuildInfo(var V1, V2, V3, V4: word);
  function GetBuildInfoAsString(aFileName: string = ''): string;


//var
//  L_AppName: string;            // zie AAUtil.G_AppName
  //function GetDataPath: string;  // zie AAUtil.G_DataPath

implementation

uses Windows, Math, Variants, ShlObj,{$IFNDEF DLL}Vcl.Forms{$ENDIF};

const
  L_AppIniFile    : TAAIniFile = nil;

function INStringsCreate(UseStringList:TStringList=nil): TStringsIntfImpl;
begin
  Result:= TStringsIntfImpl.Create(UseStringList);
end;

function  Copy1( S: String; Index: Integer; DefaultCar: Char ): Char;
// Index -1..-L telt van achteruit
var l: integer;
begin

  L:= Length(S);
  if Index<0 then begin
    Index:= L+Index+1;
    if Index<=0 then Result:= DefaultCar
    else Result:= S[Index];
  end
  else begin
    if (Index=0) or (Index>L) then Result:= DefaultCar
    else Result:= S[Index];
  end;

end;


function CopyProps( Src, Dest: TObject; PropNames : array of string ): integer;
// properties copieren, PropName=[SrcProp1;DestProp2,SrcProp1;DestProp2]
var      i,p,idx: integer; obj, lobj2: TObject; pnSrc, pnDest: string;
  PropInfo, PropInfoD: PPropInfo; V: Variant;
  SrcList: PPropList;
begin

  if high(PropNames)<0 then begin                                               // just copy all props
    i:= 0;
    SrcList  := AllocMem(SizeOf(SrcList^));
    try
      GetPropList(Src.ClassInfo,tkSimpleProperties,SrcList);
      //GetPropList(Source.ClassInfo,tkSimpleProperties,DestList);
      while (SrcList^[i]<>nil) and (i<High(SrcList^)) do begin
        if GetPropInfo(Dest,SrcList^[i].Name,tkSimpleProperties)<>nil then
          try
            PropInfoD:= GetPropInfo(Dest,SrcList^[i].Name,tkSimpleProperties);
            if PropInfoD.SetProc=nil then begin
              inc(i); continue;
            end;
            if PropInfoD.PropType^^.Kind in [tkString, tkLString,tkWString] then
              V:= GetPropValue(Src,SrcList^[i].Name,True)
            else
              V:= GetPropValue(Src,SrcList^[i].Name,False);
            SetPropValue(Dest,SrcList^[i].Name,V);
            //CheckSetPropValue(Dest,[SrcList^[i].Name],CheckPropValue(Src,[SrcList^[i].Name],False));
          except
          end;
        Inc(i);
      end;
      FreeMem(SrcList);
    except
      FreeMem(SrcList);
    end;
    end
  else begin
    i:= 0;
    while (i<=high(PropNames)) do try
      p:= pos(';',PropNames[i]);
      if p=0 then begin
        V:= GetPropValue(Src,copy(PropNames[i],1,p-1),False);
        SetPropValue(Dest,copy(PropNames[i],p+1,99),V);
        end
      else begin
        V:= GetPropValue(Src,copy(PropNames[i],1,p-1),False);
        SetPropValue(Dest,copy(PropNames[i],p+1,99),V);
      end;
    except
    end;
  end;
end;

function PropertyObject(Instance:TObject;var PropertyPath:string; AllowObjectPropReturn: Boolean= True ): TObject;
// maak lines.text -> obj=lines, result=text
var      i,p,idx: integer; obj, lobj2: TObject; PropName: string;
  PropInfo: PPropInfo;
begin

  Result:= Instance;
  while true do begin
    p:= pos('.',PropertyPath);
    if p>0 then PropName:= Copy(PropertyPath,1,p-1) else PropName:= PropertyPath;
    PropInfo:= GetPropInfo(Result,PropName);
    if (PropInfo<>nil) and (PropInfo.PropType^^.Kind=tkClass) and (AllowObjectPropReturn or (posv('.',PropertyPath,p+1)>0)) then begin                            //
      Result:= GetObjectProp(Result,PropInfo);
      if p<>0 then Delete(PropertyPath,1,p)
      else PropertyPath:= '';
    end
    else begin
      break;
    end;
  end;

end;


function CheckPropValue( Instance: TObject; PropNames: array of string; PreferStrings: Boolean = True ): Variant;
// preferstrings geeft indien mogelijk een string terug, anders anders integer, object
// komt uit aautil echter geeft conflict met aadatadic
var      i,p,idx: integer; obj, lobj2: TObject; PropName: string;
  PropInfo: PPropInfo;
begin

  Result := Null;
  i:= 0;
  while (i<=high(PropNames)) do begin
    PropName:= PropNames[i];
    obj:= PropertyObject(Instance,PropName);
    if PropName='' then begin                                                   // target is een object
      if Result=Null then Result:= Integer(obj);                                // object result gebruiken indien andere failen
      if PreferStrings then
        if (GetPropInfo( obj,'Text')<>nil) then Result:= TypInfo.GetPropValue(obj,'Text',PreferStrings)
        else
          if obj.inheritsfrom(TStrings) then Result:= TStrings(obj).Text
          else begin inc(i); continue; end
      else Result:= Integer(obj); // Integer(GetObjectProp(obj,PropInfo))
      end
    else begin
      PropInfo:= GetPropInfo(obj,PropName);                                     // target is NIET object
      if (PropInfo<>nil) then Result:= TypInfo.GetPropValue(obj,PropName,PreferStrings)
      else if (PropName='Text') and obj.inheritsfrom(TStrings) then Result:= TStrings(obj).Text // Lines.Text
      else begin
        inc(i);                                                                 // volgende zoeken
        continue;
      end;
    end;
    exit;
    //inc(i);
  end;
  //  failure
  //Result := Null;

end;


function CheckSetPropValue( Instance: TObject; PropNames: array of string; Value: Variant ): boolean;
// Result = True indien gevonden
// komt uit aautil echter geeft conflict met aadatadic
var      i,p,idx: integer; obj, lobj2: TObject; PropName: string;
  PropInfo: PPropInfo;
  const csDropdownList= 2 shl 3;
begin

  i:= 0;
  while (i<=high(PropNames)) do begin
    PropName:= PropNames[i];
    obj:= PropertyObject(Instance,PropName);
    if PropName='' then                                                         // target is een object
      if ((VarType(Value)=varStrArg) or (VarType(Value)=varOleStr) or (VarType(Value)=varString)) then
        if (GetPropInfo(obj,'Text')<>nil) then	SetStrProp(obj,'Text',Value)
        else
          if obj.InheritsFrom(TStrings) then TStrings(obj).Text:= Value
          else begin inc(i); continue; end
      else begin
        PropName:= PropNames[i]; obj:= PropertyObject(Instance,PropName,False);
        SetObjectProp(obj,PropName,TObject(Integer(Value))); end
    else begin
      PropInfo:= GetPropInfo(obj,PropName);                                     // target is NIET object
      if PropInfo<>nil then
        if SameText('Text',PropName) and (GetPropInfo(obj,'ItemIndex')<>nil) and (csDropdownList=GetPropValue(obj,'Style',False))  then begin
          PropInfo:= GetPropInfo( obj, 'Items' );                                 // ItemIndex property instellen
          if (PropInfo<>nil) then lobj2:= GetObjectProp(obj,PropInfo)
          else lobj2:= nil;
          if (lobj2<>nil) and lobj2.InheritsFrom(TStrings) then
            SetPropValue(obj,'ItemIndex',TStrings(lobj2).IndexOf(Value))
          else TypInfo.SetPropValue( obj,PropName,Value ); end
        else
          TypInfo.SetPropValue( obj,PropName,Value )
      else                                                                      // prop bestaat niet
        if SameText('Text',PropName) and obj.InheritsFrom(TStrings) then TStrings(obj).Text:= Value
        else begin                                                              // propinfo=nil
          inc(i);
          continue;
        end;
    end;
    Result:= True;
    exit;
    //inc(i);
  end;
  Result:= False;

end;

function Explode(s:string;separator:char): ArrayOfString;
// text1;text2 => array (text1,text2)
// moet nog aangepast worden
var i,p,l: integer; ls: string;
begin
  P:= 1; l:= length(s);
  if l=0 then begin
    SetLength(Result,0);
    Exit;
  end;
  SetLength(Result, SepFieldsCount(S,separator));
  i:= 0;
  repeat
    Result[i]:= SepFieldsNext(s,p,separator);
    inc(i);
  until i>high(Result);

end;


function iif( Conditie: boolean; Opt1: string; Opt2: string = '' ): string; overload;
begin
  if Conditie then
    Result := Opt1
  else
    Result := Opt2;
end;

function Iif(Conditie: boolean; Integer1, Integer2: Integer): Integer; overload;
{ Iif Integer }
begin
  if Conditie then
    Result := Integer1
  else
    Result := Integer2;
end;                                    {func}

function Escape( const s: string; EscapeChars: string=#0#13#10; EscapeCharsReplace: string='\0rn' ): string;
{ Maak binaire string gereed om AsString in database op te slaan
  password zo lang mogelijk maken!
  #0 vervangen door \#, \ vervangen door \\                    }
var i,j,lp,le,d:integer; esc,c: char; b: boolean;
begin

  if EscapeCharsReplace='' then raise exception.create('no escape chars!');
  esc:= EscapeCharsReplace[1]; delete(EscapeCharsReplace,1,1);
  le:= length(EscapeChars);
  //
  Result:= ''; d:= 0;
  for i:= 1 to length(s) do begin
    c:= s[i];
    if c=Esc then begin Result:= Result+Esc+Esc; inc(d); end // was '\\'
    else begin
      Result:= Result+c;
      j:= le;
      while j<>0 do begin
        if c=EscapeChars[j] then begin
          Result[i+d]:= Esc;
          Result:= Result+EscapeCharsReplace[j];
          inc(d);                                                             // delta
          Break;
        end;
        dec(j);
      end;
    end;
  end;//for

end;

function UnEscape( const S:string; EscapeChars: string=#0#13#10; EscapeCharsReplace: string='\0rn' ): string;
// remove escape chars en ook ' aan begin en einde string '
var i,j,d,l,lp,le: integer; c, esc: char;
begin

  if EscapeChars='' then raise exception.create('no escape chars!');
  esc:= EscapeCharsReplace[1]; delete(EscapeCharsReplace,1,1);
  le:= length(EscapeChars);
  l:= length(S); d:= 0; // delta
  Result:= s;
  i:= 1;
  while i<=l do begin
    if Result[i]=Esc then begin // treat next as literal
      Delete(Result,i,1); dec(l);
      // if pos( Result[i]='#' then Result[i]:= #0; origineel
      c:= Result[i];
      j:= le;
      if c<>esc then
      while j<>0 do begin
        if c=EscapeCharsReplace[j] then begin
          Result[i]:= EscapeChars[j];
          Break;
        end;
        dec(j);
      end;
    end;
    inc(i);
  end

end;

function SepFieldsAdd( const SepField, S: string; SepChar: String; IngoreDupl : boolean ): string;
begin

  if (SepField='') or (IngoreDupl and (Pos(SepChar+SepField+SepChar, SepChar+S+SepChar )>0)) then
    Result:= S
  else
    if S='' then Result:= SepField
    else Result:= S + SepChar + SepField;

end;


// start p op 1e pos van volgende veld
// post: p op start volgende:
function SepFieldsNext( FieldList: string; var P: Integer; SepChar: Char; EscapeChar: Char = ' ' ): string;
// Escape ' ' betekend none
var
  iStartPos: integer;          
  iL       : integer;
  Quote    : Char;
const
  eol = #10;
begin

  if P=0 then P:= 1;                                                            // Zoek begin van veld
  iL:= length(FieldList );
  if (P>iL) then begin          // geen velden meer -
    Result:= '';
    Exit;
    end;
  iStartPos := P;
  if FieldList[p] in ['"',''''] then begin                                      // "bla bla","blop blop" support
    Quote:= FieldList[p]; inc(p);
    while (p<=il) do
      if ((FieldList[p]=EscapeChar) and (EscapeChar<>' ')) then
        if (p=iL) then break                                                    // eol
        else inc(p,2)
      else
        if (FieldList[p]=Quote) then break                                      // end quote
        else inc(p);
  end
  else begin
    // Zoek einde van veld                                                      // bla bla,blop blop
    //P:= posv( SepChar, FieldList, P );
    //if P=0 then P:= iL + 1;
  end;

  // zoek einde veld, misschien moet dit met escape
  while (p<=il) and (FieldList[p]<>SepChar) and (FieldList[p]<>eol) do inc(p);  // skip till ','

  Result:= Copy( FieldList, iStartPos, P-iStartPos );
  Inc( P );                                                                     // skip SepChar

end;

function SepFieldsCount(FieldList: string; SepChar: Char; EscapeChar: Char = ' '):integer;
var p1,p2: integer;
begin
  p1:= 1; p2:= p1;
  SepfieldsNext(FieldList,p1,SepChar,EscapeChar);
  Result:= 0;
  while p1<>p2 do begin
    inc(Result);
    p2:= p1;
    SepfieldsNext(FieldList,p1,SepChar,EscapeChar);
  end;
end;

function SepFieldsLine(const FieldList: string; var P: Integer; aData:TStrings; SepChar: Char; EscapeChar: Char = ' '; Eol:string=#13#10 ): boolean;
//function SepFieldsLine(const FieldList: string; var P: Integer; aData:TStrings; SepChar: Char; Eol:string='<br>'#13#10; EscapeChar: Char = ' '; ): boolean;
// lees een regel van een csv file
var i,iEol,lEol,pold,iL: integer; sVal: string;
begin

  //if EscapeChars='' then EscapeChar:= ' '
  //else EscapeChar:= EscapeCharsReplace[1];
  Result:= False;
  lEol:= length(Eol); iEol:= pos(#10,Eol); if iEol=0 then iEol:= 1;
  iL:= Length(FieldList); i:= 0; aData.Clear;
  pold:= p;
  sVal:= SepFieldsNext(FieldList,P,SepChar,EscapeChar);
  while p<>pold do begin
    inc(i);
    if (FieldList[p-1]=Eol[ieol]) then
      if copy(FieldList,p-ieol,lEol)=Eol then begin
      Result:= i>0;                                   // 2010-2 gewijzigd
      sVal:= copy(sVal,1,length(sVal)-iEol+1);
      //sVal:= UnEscape(sVal,EscapeChars,EscapeCharsReplace);
      aData.Add(sVal);
      P:= p+length(Eol)-iEol;
      exit;
    end;
    //sVal:= UnEscape(sVal,EscapeChars,EscapeCharsReplace);
    aData.Add(sVal);
    pold:= p;
    sVal:= SepFieldsNext(FieldList,P,SepChar,EscapeChar);
  end;

end;

procedure SepFieldsUnEscape( aData:TStrings; EscapeChars:string='"'; EscapeCharsReplace:string='""'; QuoteChars:string='""' );
// unquote en unescape Val = " es\'caped " : quote escapedtxt quote
var i,l: integer; sVal:string;
begin

  if length(QuoteChars)=1 then begin
    QuoteChars:= QuoteChars+QuoteChars;
    //raise exception.create('QuoteChars must be a pair');
  end;
  for i:= 0 to aData.Count-1 do begin
    sVal:= aData[i];
    l:= length(sVal);
    if (l=0) or (l=1) then continue;
    if EscapeCharsReplace='' then begin
      if (sVal[1]=QuoteChars[1]) and (sVal[l]=QuoteChars[2]) then
        aData[i]:= copy(sVal,2,l-2)
      else
        aData[i]:= sVal;
    end
    else begin
      if (sVal[1]=QuoteChars[1]) and (sVal[l]=QuoteChars[2]) then
        aData[i]:= UnEscape(copy(sVal,2,l-2),EscapeChars,EscapeCharsReplace)
      else
        aData[i]:= UnEscape(sVal,EscapeChars,EscapeCharsReplace);
    end;
  end;

end;

function Str2DateTime(const Value: string): TDateTime;
// [yy]yy[-][m]m[-][d]d formaat
// dd-mm-yyyy formaat
var
  Year, Month, Day, Hour, Min, Sec, MSec, E: Integer;
  StrLength, StrPos, StrPosPrev: Integer; TwoDigitYear: boolean;
  //
  function CharMatch1( matchchars: TCharSet ): boolean;
  begin
    StrPosPrev:= StrPos;
    while StrPos<=StrLength do
      if Value[StrPos] in matchchars then inc(StrPos)
      else break;
    Result:= StrPosPrev<>StrPos;
  end;
begin

  Result := 0;
  StrPos:= 1;
  StrLength := Length(Value);

  if not CharMatch1(['0'..'9']) then exit;                         // year
  if (StrPos-StrPosPrev>=6) then begin
    if (StrPos-StrPosPrev>=8) then begin
      Val(Copy(Value, StrPosPrev, 4), Year, E);
      inc(StrPosPrev,4);
      end
    else begin
      Val(Copy(Value, StrPosPrev, 2), Year, E);
      if Year<50 then inc(Year,2000) else inc(Year,1900);
      inc(StrPosPrev,2);
    end;
    Val(Copy(Value, StrPosPrev, 2), Month, E);
    inc(StrPosPrev,2);
    Val(Copy(Value, StrPosPrev, 2), Day, E);

    inc(StrPosPrev,2);
    Result := EncodeDate(Year, Month, Day);
  end
  else begin                                                       // 2009-/\02-/\02 support
  Val(Copy(Value, StrPosPrev, StrPos-StrPosPrev), Year, E);
  if (StrPos>StrLength) or (Value[StrPos]<>':') then try                         // it is a date..
    TwoDigityear:= (StrPos-StrPosPrev)<=2;
    if CharMatch1(['-','/','\']) and CharMatch1(['0'..'9']) then   // month
      Val(Copy(Value, StrPosPrev, StrPos-StrPosPrev), Month, E)
    else Month:= 1;
    if CharMatch1(['-','/','\']) and CharMatch1(['0'..'9']) then   // day
      Val(Copy(Value, StrPosPrev, StrPos-StrPosPrev), Day, E)
    else Day:= 1;
    // day
    if Day>31 then begin // 01-01-2009 support...
      Result := EncodeDate(Day, Month, Year);
    end
    else begin
      if TwoDigitYear then if Year<50 then inc(Year,2000) else inc(Year,1900);
      Result := EncodeDate(Year, Month, Day);
    end;
  except
  end
  else
    Result:= 0;
  end;
  //
  if (StrPos<=StrLength) then try
    if (Value[StrPos]=':') then begin       // geen datum, alleen tijd
      //Hour:= Year;
      //StrPos:= StrPosPrev;
      end
    else begin
      if not CharMatch1([' ','T']) then exit;
      if not CharMatch1(['0','1','2','3','4','5','6','7','8','9']) then exit;       // hour
    end;
    //
    if (StrPos-StrPosPrev)>2 then begin
      Hour := StrToIntDef(Copy(Value,StrPosPrev,2),0);                          // zonder : ertussen
      Min  := StrToIntDef(Copy(Value,StrPosPrev+2,2),0);
      Sec  := StrToIntDef(Copy(Value,StrPosPrev+4,2),0);
    end
    else begin
      Hour:= StrToIntDef(Copy(Value, StrPosPrev, StrPos-StrPosPrev), 0);

      if CharMatch1([':']) then begin
        if CharMatch1(['0'..'9']) then // minute
          Val(Copy(Value, StrPosPrev, StrPos-StrPosPrev),Min,E)
        else Min:= 0;
        if CharMatch1([':']) and CharMatch1(['0'..'9']) then // second
          Val(Copy(Value, StrPosPrev, StrPos-StrPosPrev),Sec,E)
        else Sec:= 0;
        {if CharMatch1([':','.']) and CharMatch1(['0'..'9']) then // milli second support
          Val(Copy(Value, StrPosPrev, StrPos-StrPosPrev),MSec,E)
        else}
        MSec:= 0;
      end
      else begin
        Min:= 0;
        Sec:= 0;
        MSec:= 0;
      end;
    end;
    if Result >= 0 then
      Result := Result + EncodeTime(Hour, Min, Sec, MSec)
    else
      Result := Result - EncodeTime(Hour, Min, Sec, MSec)
  except
  end;

end;

function Pad(S: string; targetLength: integer; filchar: char = ' '): string;
{ pad een string aanvullen met spaties tot lenght resultlength }
var
  i, l: integer;
begin
  Result := s;
  l := length(Result);
  //if targetlength > l then
  begin                                 // aanvullen met spaties
    SetLength(Result, targetlength);
    for i := l + 1 to targetlength do
      Result[i] := filchar;
  end;
end;

function Zero(S: string; targetLength: integer; filchar: char = ' '): string;
{ pad een string aanvullen met spaties tot lenght resultlength }
var
  i, l: integer;
begin

  Result:= '';
  l := targetLength-length(s);
  SetLength(Result,l);
  // FillChar(Result[1],l,Word(filchar)); problems
  for i := 1 to l do Result[i]:= filchar;
  Result := Result+s;
  SetLength(Result, targetlength);

end;

function posv(substr, s: string; vanaf: integer): integer;
{ doe een pos vanaf een bep plaats, niet gevonden dan Result:= 0!! }
var i,l,lsub: integer;
begin
  if vanaf>=0 then begin
    s := Copy(s, vanaf, length(s) - vanaf + 1);
    Result := pos(substr, S);
    if Result <> 0 then Inc(Result, vanaf - 1);
  end
  else begin                                    // vanaf < 0  =>  achteruit zoeken
    l:= length(s); lsub:= length(substr);
    if vanaf<-l then vanaf:= -l;
    for i:= l-lsub+1 downto l+vanaf do
      if (substr[1]=s[i]) and (substr=copy(s,i,lsub)) then begin
        Result:= i; exit;
      end;
    Result:= 0;
  end;
end;

function BrowseCallbackProc(hwnd: HWND; uMsg: UINT; lParam: LPARAM; lpData: LPARAM): Integer; stdcall;
begin
  if (uMsg = BFFM_INITIALIZED) then
    SendMessage(hwnd, BFFM_SETSELECTION, 1, lpData);
  BrowseCallbackProc := 0;
end;

function SelDir( var strFolder: string; Caption: string ): Boolean;
begin
  raise Exception.Create('function SelDir not implemented');
end;

function txt2float(const S: string; var aPos: integer; DecimalSep:char='.'; ThousendSep:char='0' ): double;
// vis een [-]99.999,99 getal uit een string
//pre: 0 betekend geen thousend separator
//post: aPos = -1 betekend none found
var
  P,P2,L: integer;
begin
  L:= Length(S);
  Result:= 0;
  while (aPos<=L) and not(S[aPos] in ['0'..'9']) do inc(aPos);
  if aPos>L then begin
    aPos:= -1;
    exit; //raise exception.create('Error txt2float: no numeric found');   // geen getal gevonden
  end;

  P:= aPos;                                             // aPos staat nu op eerste cijfer
  while (aPos<=L) and (S[aPos] in ['0'..'9',ThousendSep]) do begin
    inc(aPos);
  end;
  if copy1(S,P-1)='-' then Result:= -strtoint(copy(S,P,aPos-P))                 // deel voor comma
  else Result:= strtoint(copy(S,P,aPos-P));

  if copy1(S,aPos)<>DecimalSep then exit; // geen DecimalSep

  inc(aPos); P2:= aPos;
  while (aPos<=L) and (S[aPos] in ['0'..'9']) do begin
    inc(aPos);
  end;
  if aPos=P2 then exit;

  if copy1(S,P-1)='-' then Result:= Result - strtoint(copy(S,P2,aPos-P2)) / Power(10,aPos-P2)
  else Result:= Result + strtoint(copy(S,P2,aPos-P2)) / Power(10,aPos-P2)
end;

function Str2IntDef(const S: string; const def: integer=0 ): integer;
begin
  if(S='') then Result:= def
  else Result:= StrToInt(S);
end;


function Float2Str( Value: Extended; Digits: Integer=2; aDecimalSeparator: Char=','; aThousandSeparator: Char='N' ): string;
{ conversion without formatting, do not use for user output
  if Digits=0 then round to integer values, Precision should be Digits + Integers
  if Digits=-1 then try to include as much as needed
  if ThousendSeparator='N','F' then no thousend separator
  rem. if number bigger then precision you get 8,343434E19
}
var
  Precision: Integer; Remove0Decimals:boolean; l: integer; LFormat: TFormatSettings;
  Buffer: array[0..255] of Char;
begin
  Precision:= 20;
  {if Digits=0 then begin
    Result:= IntToStr( Round(Value) );
    Exit;
  end;}
  LFormat.DecimalSeparator:= aDecimalSeparator;
  if Digits=-1 then begin                       // Float ',' to max precision
    Digits:= 20;
    Remove0Decimals:= True;
  end
  else Remove0Decimals:= False;

  // ffFixed,ffNumber: Keep digits after ','
  if aThousandSeparator in ['N','F'] then
    l:= FloatToText(Buffer, Value, fvExtended, ffFixed, Precision, Digits, LFormat )
  else begin
    LFormat.ThousandSeparator:= aThousandSeparator;
    l:= FloatToText(Buffer, Value, fvExtended, ffNumber, Precision, Digits, LFormat );
  end;

  SetString(Result, Buffer, l);
  if Remove0Decimals then
    while (l>0) and (Result[l]='0') do begin
      Delete(Result,l,1);
      dec(l);
    end;

end;


function Const2Str( const Value: TVarRec; FloatFormat:string='0.##' ): String;
// FloatFormat='dd-mm-yy' formats a date
//             'l'/'L' local format
//             [['],]'3' means ' as thousendsep, ',' as decimalsep, 3 digits
//Float wordt geformateerd zoals aangegeven in FloatFormat, laatste ./, is decimaal teken
var t,d,p,i,j,pr,dc: integer; dp,ts: char; L: Int64;
procedure error;
  begin
    raise EConvertError.Create( 'Const2Variant: invalid conversion' );
  end;
  var v: variant;
begin
{
  V:= Const2Variant( Value );
  Result:= String( V );
  Exit;
}
  with Value do
    case VType of
      vtInteger:
        Result := IntToStr( VInteger );
      vtBoolean:
        Result := iif( VBoolean, 'True', 'False' );
      vtChar:
        Result := String(VChar);
      vtExtended:
        if (FloatFormat[1] in ['h','d','m','y']) then Result:= FormatDateTime(FloatFormat, VExtended^ ) else
        if (FloatFormat[1] in ['l','L']) then Result:= FloatToStr(VExtended^) else      // local format
        if (FloatFormat[1] in ['0','#']) then begin
          Result:= FormatFloat(FloatFormat,VExtended^{,Format});
        end
        else begin          // [['],]'3' means ' as thousendsep, ',' as decimal sep, 3 digits
          dp:= ' '; j:= length(FloatFormat);
          d:=j; while (d>0) and not (copy1(FloatFormat,d) in ['.',',']) do dec(d);
          if d=0 then begin             // geen ',' dan rounded integer
            if j=0 then dc:= 0
            else dc:= Strtoint(FloatFormat);
            //Result:= Result + FormatFloat('0.'+Zero('#',dc,'#'),VExtended^);
            Result:= Float2Str(VExtended^,dc,',','N');
          end
          else begin                   // precision,digits
            pr:= StrToInt(copy(FloatFormat,1,d-1));
            dp:= FloatFormat[d];
            if d=2 then ts:= FloatFormat[d-1] else ts:='N';
            if d=j then dc:= -1
            else dc:= StrToInt(copy(FloatFormat,d+1,2));
            Result:= Float2Str(VExtended^,dc,dp,ts);
          end;
        end;
      vtString: begin
        Result := String(VString^); // Dit is link in verband met reference counting
        end;
      vtPointer: begin
        // TVarData(Result).VType    := varByRef + varEmpty; // gebruik ik pointer!!!
        // TVarData(Result).VPointer := VPointer;
        Result := InttoStr( Integer( VObject ) );
        end;
      vtPChar:
        Result := string( VPChar );
      vtObject: begin
        // Als we varUnknow opgeven kunnen we geen v1=v2 doen; gebruik SimpleVariantCompare
        Result := InttoStr( Integer( VObject) );
        // TVarData(Result).VType     := varEmpty; // varInteger; // Gebruik ik pointer!!!
        // TVarData(Result).VPointer  := VObject;
        end;
      vtClass    :
        Result:= VClass.ClassName;
      vtWideChar :        Result:= String(VWideChar);
      vtPWideChar:        Result:= String(VPWideChar);
      vtAnsiString:
        Result := string(VAnsiString);
      vtCurrency:
        Result := Format( '%f', [VCurrency^] );
      vtVariant:
        if not VarIsEmpty(VVariant^) then
          Result := VarToStr(vVariant^) // String( vVariant^ ) // Var2Str( VVariant^ )          // Bij Array variants gaat dit in ieder geval mis!
        else
          Result := '';
      vtInterface:
        Result:= 'Interface';// + IUnknown( VInterface ). ;
      vtWideString:     Result:= String(VWideString);
      vtUnicodeString:  Result:= String(VUnicodeString);
      vtInt64:        Result:=  IntToStr( VInt64^ );
      255 :        Result:= String(VPWideChar);    else
      Error;
    end;
end;

function Fmt( s:string; params:array of const; QuoteChar:Char='''' ):string;
// noramlly used for formatting user messages
// replace %x with formatted value
// %s format general ;
// %3 format with 3 decimals ;
// %d[t] format as date: 2010-03-03 [12:03:12];
// %t[l] format as time: 12:03[:11]
// %q format quotes: 'Param1';
// %4[.2] format indent to 4, use 2 decimals
var i,l,la,ip: integer;
begin
  Result:= '';
  ip:= -1;
  l:= length(s);
  la:= high(params);
  i:= 1;
  while i<l do
    if (s[i]='%') and (s[i+1]<>'%') then begin
      inc(ip);
      if ip>la then begin
        ShowMsg(g_app,'Fmt: param missing',SM_OPTABORT);
        Result:= Result+copy(s,i,999);
      end;
      inc(i);
      case s[i] of
      'd','D': if s[i+1] in ['t','T'] then Result:= Result+Const2Str( params[ip], 'dd-mm-yyyy hh:nn:ss' )
               else Result:= Result+Const2Str( params[ip], 'dd-mm-yyyy' );
      't','T': if s[i+1] in ['l','L'] then Result:= Result+Const2Str( params[ip], 'hh:nn:ss' )
               else Result:= Result+Const2Str( params[ip], 'hh:nn' );
      'q','Q': Result:= Result+QuoteChar+Const2Str( params[ip] )+QuoteChar;
      '0'..'9': // 
        begin  // indent
          if (copy1(s,i+1) in ['.',',']) then begin
            // total.digits
            //s[i]:= '.';
            if (i+2)>l then ShowMsg(g_app,'nbr expected in float format '+s);
            Result:= Result+Zero( Const2Str( params[ip], '0,'+Pad( '0', byte(s[i+2])-byte('0'), '0'), ), byte(s[i])-byte('0') );
            inc(i,2);
            end
          else begin
            Result:= Result+Zero( Const2Str( params[ip], '#,###,##0.00', ), byte(s[i])-byte('0') );
          end;
        end;
      else begin
        Result:= Result+Const2Str( params[ip], '#,###,##0.##' );     // decimalen alleen indien aanw
      end;
      end;
      inc(i);
    end
    else begin
      Result:= Result+s[i];
      inc(i);
    end;
  if i<=l then
    Result:= Result+s[l];
end;


procedure TAAIniFile.ReadSectionValues( const Section: String; Strings: TStrings; NewNameValueSeperator: string; RemoveKey: boolean );
// voeg mogelijkheid toe om key=value te veranderen naar: key value
var i,p: integer; s:string;
begin
  ReadSectionValues(Section,Strings);
  if NewNameValueSeperator<>'=' then
    for i:= 0 to Strings.Count-1 do begin
      S:= Strings[i];
      p:= Pos('=',S);
      Delete(S,p,1); Insert(NewNameValueSeperator,S,P);
      if RemoveKey then Delete(S,1,P-1);
      Strings[i]:= S;
    end;
end;


function GetObjectDescription( Sender: TObject ): string;
begin
  if Sender=nil then Result:= ''
  else
    if Sender.InheritsFrom(TComponent) then Result:= TComponent(Sender).name+'('+Sender.ClassName+')'
    else Result:= '('+Sender.ClassName+')';
end;


procedure LogError( Sender:TObject; Msg:string );
//procedure TFormLog.LogException(Sender: TObject; E: Exception);
var
  Filename: string;
  LogFile: TextFile;
  E: Exception;
begin
  E:= Exception( ExceptObject );
  // prepares log file
  Filename := ChangeFileExt (Application.Exename, '.log');
  AssignFile (LogFile, Filename);
  if FileExists (FileName) then
    Append (LogFile) // open existing file
  else
    Rewrite (LogFile); // create a new one
  try
    // write to the file and show error
    if E<>nil then Writeln (LogFile, DateTimeToStr (Now) + ':' + Msg+' Except: '+E.Message )
    else Writeln (LogFile, DateTimeToStr (Now) + ':' + Msg );
    // if not CheckBoxSilent.Checked then Application.ShowException (E);
  finally
    // close the file
    CloseFile (LogFile);
  end;
end;


function isTerminated: boolean;
begin
  Result:= False;
  if(G_App=nil) then exit;

end;

procedure Process_Messages;
begin
  if(G_App=nil) then exit;
  // if assigned(G_OnProcessMessages) G_OnProcessMessages(G_App);
end;

{ Toont een Msg aan gebruiker, abort, aborthard genereren een exception, ahankelijk van debuglevel wordt er gelogd }
function ShowMsg( Sender: TObject; Msg: String; Code : Byte = 1; BtnInverse: boolean = FALSE ): Boolean;
// functie welke wordt aangeroepen door programmeur om MessageBox te tonen en userinterctie
var i: integer; lName: string;
begin

  if Application<>nil then begin
    // Only when its a Vcl Forms application
    Application.MessageBox(PWideChar(Msg),PWideChar(G_AppName),MB_OK)
  end;

end;

function ShowMsg( Sender: TObject; Msg: String; Params : Array of Const; Code : Byte = 1 ): Boolean;
begin
  Result:= ShowMsg( Sender, Fmt(Msg,Params), Code );
end;




{ TStringsIntfImpl }


constructor TStringsIntfImpl.create(aStringList:TStringlist=nil);
begin
  //ShowMessage('Create');
  inherited create;
  FIndex:= -1;
  FRestart:= True;
  if aStringList<>nil then FList:= aStringList
  else FList:= TAAStringList.Create;
end;

destructor TStringsIntfImpl.destroy;
begin
  //ShowMessage('Destroy');
  FList.Free;
  inherited;
end;


function TStringsIntfImpl.CompareStrings(const S1, S2: string): Integer;
begin
  if FList.CaseSensitive then
    Result := AnsiCompareStr(S1, S2)
  else
    Result := AnsiCompareText(S1, S2);
end;

function TStringsIntfImpl.Complement(aNameOrValue:string): string;
var i: integer;
begin
  i:= Flist.IndexOfName(aNameOrValue);
  if i<>-1 then begin
    Result:= Flist.ValueFromIndex[i];
    Exit;
  end
  else begin
    for i:= 0 to Flist.Count-1 do
      if CompareStrings(Flist.ValueFromIndex[i],aNameOrValue)=0 then begin
        Result:= Flist.Names[i];
        Exit;
      end;
  end;
  Result:= ''; // not found
end;

procedure TStringsIntfImpl.SetCount(const value:integer);
var i,c: integer;
begin
  c:= obj.Count;
  i:= Value-c;
  if i>0 then
    while i<>0 do begin
      obj.add('');
      dec(i);
    end
  else if i<0 then
    while c<>value do begin
      dec(c);
      obj.Delete(c);
    end;
end;

function TStringsIntfImpl.obj: TStringList;
begin

  Result:= FList;
end;                                    {func}

function TStringsIntfImpl.Loop(LoopForward:boolean=True): boolean;
{ gebruik: while Loop do .... }
begin

  Result:= True;
  if FRestart then begin      // zet op begin van loop
    //Application.ProcessMessages;
    //if Application.Terminated and (FList.Count<>0) then ShowMsg(self,'User terminated app',sm_abort);
    FRestart:= False;
    FReset:= False;
    if LoopForward then FIndex:=0
    else FIndex:= FList.Count-1;
    Exit;
  end
  else
  if FReset then begin        // blijf 1 ronde op aangewezen plaats
    FReset:= False;
    Exit;
  end;

  if LoopForward then begin
    inc(FIndex);
    if FIndex>=fLiST.Count then begin // einde loop
      dec(FIndex);
      Result:= False;
      FRestart:= True;
    end
  end
  else begin
    Dec(FIndex);
    if FIndex<=-1 then begin    // einde loop
      FIndex:= 0;
      Result:= False;
      FRestart:= True;
    end;
  end;

end;

function TStringsIntfImpl.LoopIndex: Integer;
begin
  Result:= FIndex;
end;

function TStringsIntfImpl.LoopValue: string;
begin
  Result:= Obj[FIndex];
end;



{ TEventList }

constructor TEventListIdx.Create(aSilentOnEventNotFound: boolean);
begin
  EventList:= TStringList.Create;
  EventList.Sorted:= True;
  SilentOnEventNotFound:= aSilentOnEventNotFound;
end;

destructor TEventListIdx.Destroy;
var
  i:integer;
begin
  for i := 0 to EventList.Count-1 do
    Dispose(PNotifyEvent(EventList.Objects[i]));
  EventList.Free;
  inherited;
end;

function TEventListIdx.GetEventCount:integer;
begin
  result := EventList.Count;
end;

procedure TEventListIdx.AddNotifyEvent( aName:string; ne:TNotifyEvent );
// add( 'AfterInsert', ProcName )
var
  p:PNotifyEvent;
  i:integer;
begin
  for i := 0 to EventList.Count-1 do
    if @TNotifyEvent(PNotifyEvent(EventList[i])^) = @ne then exit;
  New(p);
  p^ := ne;
  EventList.AddObject(aName,tobject(p))
end;

procedure TEventListIdx.DeleteNotifyEvent(aName:string);
var
  i:integer;
begin
  if EventList.Find(aName,I) then
    Dispose(PNotifyEvent(EventList[i]));

{


  for i := 0 to EventList.Count-1 do
    if @TNotifyEvent(EventList[i]^) = @ne then
    begin
      Dispose(PNotifyEvent(EventList[i]));
      EventList.Delete(i);
      break;
    end;
}
end;

procedure TEventListIdx.Fire( aName: string; Sender: TObject = nil; SilentOnException: boolean = False );
var
  i,idx,imax:integer;
begin

  if sender=nil then sender:= self;
  if not EventList.Find(aName, idx) then begin
    if not SilentOnEventNotFound then ShowMsg(self,'Event not found:'+aName);
    exit;
  end;
  //
  imax:= EventList.Count-1;
  while (idx<=imax) and (aName=EventList[idx]) do begin
    if Assigned(EventList.Objects[idx]) then
      if SilentOnException then try
        PNotifyEvent(EventList.Objects[idx])^(sender)
      except
      end
      else
        PNotifyEvent(EventList.Objects[idx])^(sender);
    inc(idx);    
  end;
end;


procedure TStringsIntfImpl.ParseCsv(FieldList: string; var P: Integer; SepChar: Char; EscapeChar: Char=' '; QuoteChars:string='"''' );
// Escape ' ' betekend none
var
  iStartPos: integer;
  iP,iL       : integer;
  Quote    : Char;
begin
{
  if P=0 then P:= 1;                                                            // Zoek begin van veld
  iL:= length(FieldList );

  while P<>PL
  if (P>iL) then begin          // geen velden meer -
    Result:= '';
    Exit;
    end;
  iStartPos := P;
  if pos(FieldList[p],QuoteChars)>0 then begin                                  // "bla bla","blop blop" support
    Quote:= FieldList[p]; inc(p);
    while (p<=il) do
      if ((FieldList[p]=EscapeChar) and (EscapeChar<>' ')) then
        if (p=iL) then break                                                    // eol
        else inc(p,2)
      else
        if (FieldList[p]=Quote) then break                                      // end quote
        else inc(p);
    while (p<=il) and (FieldList[p]<>SepChar) do inc(p);
  end
  else begin
    // Zoek einde van veld                                                      // bla bla,blop blop
    P:= posv( SepChar, FieldList, P );
    if P=0 then P:= iL + 1;
  end;
  Result:= Copy( FieldList, iStartPos, P-iStartPos );
  Inc( P );                                                                     // skip SepChar
}
end;

function TStringsIntfImpl.GetLoopObject: TObject;
begin
  Result:= FList.Objects[FIndex];
end;

function TStringsIntfImpl.Get_Text: String;
begin
  Result:= FList.Text
end;

function TStringsIntfImpl.GetValue(const Name: string): string;
begin
  Result:= FList.Values[Name];
end;

procedure TStringsIntfImpl.SetLoopObject(value: TObject);
begin
  FList.Objects[FIndex]:= Value;
end;

procedure TStringsIntfImpl.Set_Text(const value: string);
begin
  self.FList.Text:= value;
end;

procedure TStringsIntfImpl.SetValue(const Name, Value: string);
begin
  FList.Values[Name]:= Value;
end;

procedure TStringsIntfImpl.Reset(LoopForward:boolean=True;aIndex:Integer=-1);
// Zet loop terug naar start
begin
  if aIndex<>-1 then FIndex:= aIndex
  else
    if LoopForward then FIndex:=0
    else FIndex:= FList.Count-1;
  FRestart:= False;
  FReset:= True;
end;

{ TAAStringList }

function TAAStringList.Find(const S: string; var Index: Integer): Boolean;
begin
  if Sorted then Result:= inherited Find(S,Index)
  else begin
    Index:= IndexOf(S);
    Result:= Index<>-1;
  end;
end;

function  Files2List(const StartDirAndMask: String; const List: TStrings): Boolean;
var
  SRec: TSearchRec;
  Res: Integer;
begin
  if not Assigned(List) then
  begin
    Result := False;
    Exit;
  end;
  Res := SysUtils.FindFirst(StartDirAndMask + '*.*', faAnyfile, SRec );
  if Res = 0 then
  try
    while res = 0 do
    begin
      if (SRec.Attr and faDirectory <> faDirectory) then
        // If you want filename only, remove "StartDir +"
        // from next line
        List.Add( SRec.Name );
      Res := SysUtils.FindNext(SRec);
    end;
  finally
    SysUtils.FindClose(SRec);
  end;
  Result := (List.Count > 0);
end;

// var s: string;

procedure GetBuildInfo(var V1, V2, V3, V4: word; aFileName: string = '');
var
  VerInfoSize, VerValueSize, Dummy: DWORD;
  VerInfo: Pointer;
  VerValue: PVSFixedFileInfo;
  strFileName: string;
begin
  strFileName := ParamStr(0);
  if Length(aFileName) > 0 then
    strFileName := aFileName;
  VerInfoSize := GetFileVersionInfoSize(PChar(strFileName), Dummy);
  if VerInfoSize > 0 then
  begin
      GetMem(VerInfo, VerInfoSize);
      try
        if GetFileVersionInfo(PChar(strFileName), 0, VerInfoSize, VerInfo) then
        begin
          VerQueryValue(VerInfo, '\', Pointer(VerValue), VerValueSize);
          with VerValue^ do
          begin
            V1 := dwFileVersionMS shr 16;
            V2 := dwFileVersionMS and $FFFF;
            V3 := dwFileVersionLS shr 16;
            V4 := dwFileVersionLS and $FFFF;
          end;
        end;
      finally
        FreeMem(VerInfo, VerInfoSize);
      end;
  end;
end;

function GetBuildInfoAsString(aFileName: string = ''): string;
var
  V1, V2, V3, V4: word;
begin
  GetBuildInfo(V1, V2, V3, V4, aFileName);
  Result := IntToStr(V1) + '.' + IntToStr(V2) + '.' +
    IntToStr(V3) + '.' + IntToStr(V4);
end;

var moduleNameP: array[0..1023] of char;

initialization

  GetModuleFileName(hInstance, moduleNameP, 1024);
  G_AppName:= ExtractFileName(moduleNameP);

  //L_Appname := ChangeFileExt( ExtractFileName(ParamStr(0)), '' );
  G_DataPath:= ExtractFilePath(ParamStr(0));
  //G_AppName:= ExtractFileName(ParamStr(0));
  //G_AllowLocalMsg := True;
  //G_DebugLevel := 255 - SM_OK - SM_YESNO;
  G_SyncGlobalObjects:= TMultiReadExclusiveWriteSynchronizer.Create;

finalization

  // if L_AppIniFile<>nil then FreeNil(L_AppIniFile);
  G_SyncGlobalObjects.Free;
  G_SyncGlobalObjects:= nil;

end.


