unit NsDatasetCache;
{:********************************************************************
*  NSDataCtrl  <br/>
*  all System  <br/>
*  Date 01.10.2017 <br/>
*  Copyright 2010 by MicroObjects, Licence given to Ergomed Plc  <br/>
*  @author Niek Sluijter <br/>
*  @Desc  Dataset cache with indexes and
**********************************************************************}
{
Fast readonly dataset replacement (can be used for cache)
}
interface

uses SysUtils, Classes, Db;

type
  TIntArray= Array of Integer;
  TDatasetCacheTS = class;
  TDatasetCacheRowList = class;
  TDatasetCacheIndexes= class;
  TDatasetCacheRow = class(TPersistent)
  protected
    function GetS(field: string): string;
    procedure SetS(field: string; const Value: string);
    function getI(field: string): integer;
    procedure SetI(field: string; const Value: integer);
  private
    function GetAsString: string;
    // represend an row in the dataset
  protected
    FOwner: TDatasetCacheTS;
    FieldIndexes: TIntArray;        // fieldend index (1 after last position)
    data: string;                   // all fields glued together in string format
    FIndexes: TDatasetCacheIndexes;
    procedure AddField(atIndex:Integer=-1);
  public
    constructor Create(aOwner:TDatasetCacheTS); virtual;
    destructor Destroy; override;
    procedure setData(const D:TDataset); virtual;
    procedure Assign(Source: TPersistent); override;
    procedure setField(const index:integer; const Value:string); virtual;
    function getField(const index:integer): string; virtual;
    function getFieldI(const index:integer): integer; virtual;
    function GetIndex(const IndexName:string; createIfNotExists:boolean) : TDatasetCacheRowList;
    function Info: string;
    function id: integer;
    //
    property S[field:string]: string read GetS write setS; default;
    property I[field:string]: integer read getI write setI;
    property AsString: string read GetAsString;
  end;
  TDatasetCacheRowClass= class of TDatasetCacheRow;


  TDatasetCacheRowOpt_= class(TDatasetCacheRow)
  public
    // optimized storage: integer
    function getField(const index:integer): string; override;
    function getFieldI(const index:integer): integer; override;
    procedure setField(const index:integer; const Value:string); override;
    procedure setData(const D:TDataset); override;
  end;
  TDatasetCacheRowOpt= TDatasetCacheRowOpt_;
  { A list of Rows with alternative indexing, not owning objects }
  TDatasetCacheRowList= class(TStringlist)
  public
    indexfield: string;       // property
    ignoreempty: boolean;     // dont add empty key
    FLastFindCache: boolean;  // default false
    FLastFindKey: string;
    FLastFind: TDatasetCacheRow;
    procedure AddRow(obj:TDatasetCacheRow;keyvalue:string;Check4Existing:boolean=False);
    function Loop(var i:Integer): boolean;
    function getRow(index:integer): TDatasetCacheRow;
    procedure Assign(SourceObj:TObject;csvIndexFields:string);
    function Row4key(key:string): TDatasetCacheRow;
    procedure Changed; override;
    constructor Create;
    property Row[index:integer]: TDatasetCacheRow read getRow; default;
  end;

  IListFacade= interface ['{E16725C5-8C81-4E05-9A73-E19F88EBE921}']
    procedure Reset();
    function Count:Integer;
    function Loop(): boolean;
    function ValS(const field:string ): string; overload;
    function ValI(const field:string): integer;
    function LoopValue: TObject;
    function Eof: boolean;
    function getLoopIndex: Integer;
    property S[const field:string]: string read ValS; default;
    property I[const field:string]: Integer read ValI;
    // procedure setLoopIndex(Value:integer);
    // property LoopIndex:integer read getLoopIndex write setLoopIndex;
  end;

  TCacheRowsIterator= class(TInterfacedObject,IListFacade)
  protected
    FLoopIndex: integer;
    FList: TStringlist;
    function ValS(const field:string ): string; overload;
    function ValI(const field:string): integer;
  public
    constructor Create();
    destructor Destroy; override;
    function Eof: boolean;
    function getLoopIndex: Integer;
    procedure setLoopIndex(Value:integer);
    procedure Reset();
    function Loop(): boolean;
    function LoopValue: TObject;
    function Count: integer;

    property LoopIndex:integer read getLoopIndex write setLoopIndex;
  end;

  TDatasetCacheIndexes= class(TStringlist)
    // FIndexes: TStringlist of array of TDatasetCacheRow;
    function getIndex(indexname:string;createIfNotExists:boolean=False): TDatasetCacheRowList;
    // readonly version of
    function getIterator(indexname:string): TCacheRowsIterator;
    // function AddObject(indexname:string;
  end;

  IDatasetCacheTS = interface ['{B8F3669B-6FB7-4A4C-A5A4-5EE3A2DA6EDE}']
    procedure Assign(const D:TDataset; const IndexField:string; const AllRecords:boolean=True);
    function Count:Integer;
    function Val(const col:integer; row:integer): string; overload;
    function FindRow(const value:string): integer;
    function getRow(const rowIdx:integer): TDatasetCacheRow;
    function Row4Id(const value: integer): TDatasetCacheRow;
    function Loop(var LoopIndex:integer): boolean;
    function Obj: TDatasetCacheTS;
  end;

  TDatasetCacheShouldAdd= function (D:TDataset): boolean of object;
  TDatasetCacheOption= ( dcoTrimFields );
  TDatasetCacheOptions= set of TDatasetCacheOption;
  { TS means it is threadsafe to loop, not for add, delete row yet }
  TDatasetCacheTS = class (TInterfacedObject,IDatasetCacheTS)
  private
    { global unique name in cachestore }
    FCacheName: string;
    { sql to load the cache }
    FSql: string;
    { scv list of fields for main index }
    FcsvIndexFields: string;
    { highest id in cache }
    FLastId: string;
  protected
    // object to loop simple dataset
    Fields : TStringlist;            //
    FieldsType: array of TFieldType; // only for Opt version
    FIndexes: TDatasetCacheIndexes;
    FLastRow: TDatasetCacheRow;
    function shouldAddRecord(D: TDataset): boolean; virtual;
  public
    Rows: TStringlist;
    ClassRow: TDatasetCacheRowClass;  // row object class to create
    // Loopindex: integer;
    constructor Create(namedcache:string);
    destructor Destroy; override;
    procedure Clear;
    procedure AssignFields(aObject:TObject);
    procedure Assign(aObject:TObject); overload;
    procedure Assign(const D:TDataset; Const csvIndexFields:string; const AllRecords:boolean=True ); overload;
    function Refresh(const D:TDataset; const UpdateRecords:boolean ): boolean;
    function AddRow(const key:string): TDatasetCacheRow;
    function GetIndex(const IndexName:string; createIfNotExists:boolean) : TDatasetCacheRowList;
    function getIterator(indexname: string; csvIndexFields:string=''; ignoreempty:boolean=True ): TCacheRowsIterator;
    function AddField( Fieldname:string; FieldType: TFieldType ): integer;
    //
    function Obj: TDatasetCacheTS;
    function Count:Integer;
    function Val(const col:integer; row:integer ): string; overload;
    function Val(const field:string; row:integer ): string; overload;
    // function ValS(const field:string ): string;
    // function ValI(const field:string ): integer;

    function FindRow(const value:string): integer; overload;
    function FindRow(const value: array of variant): integer; overload;
    function FindRow(const value: integer): integer; overload;
    function getRow(const rowIdx:integer): TDatasetCacheRow;
    function Row4Id(const value: integer): TDatasetCacheRow; overload;
    function Row4Id(const value: string): TDatasetCacheRow; overload;

    // function FindField(const field:string): integer;
    function Loop(var aloopindex:integer): boolean;
    function Info(): string;

    { sql to refresh data }
    property Sql: string read FSql write FSql;
    property LastId: string read FLastId;
    property csvIndexFields: string read FcsvIndexFields;
    property Row[const index:Integer]: TDatasetCacheRow read getRow; default;
  end;
  TDatasetCacheClass= class of TDatasetCacheTS;

  IDatasetCacheNTS = interface ['{08EC8270-791B-4D58-9F74-377C90529078}']
    procedure Assign(const D:TDataset; const IndexField:string; const AllRecords:boolean=True);
    function Count:Integer;
    function Val(const col:integer; row:integer=-1): string; overload;
    function ValS(const field:string ): string; overload;
    function ValI(const field:string): integer;
    function FindRow(const value:string): integer;
    function getRow(const rowIdx:integer): TDatasetCacheRow;
    function Row4Id(const value: integer): TDatasetCacheRow;
    function GetIndex(const IndexName:string; createIfNotExists:boolean) : TDatasetCacheRowList;
    function Loop(): boolean;
    function Obj: TDatasetCacheTS;
    property S[const field:string]: string read ValS; default;
    property I[const field:string]: Integer read ValI;
  end;


  TDatasetCacheNTS= class(TDatasetCacheTS,IDatasetCacheNTS)
  public
    FirstLoop: boolean;
    LoopIndex: integer;

    constructor Create(NamedCache:string);
    function Loop(): boolean;
    function ValS(const field:string ): string;
    function ValI(const field:string ): integer;
    //
    property S[const field:string]: string read ValS; default;
    property I[const field:string]: Integer read ValI;
  end;

  TDatasetFacade= class (TInterfacedObject,IListFacade)
    // to adapt to dataset
  public
    FD: TDataset;
    procedure Reset;
    function Eof: boolean;
    function Count: Integer;
    function Loop(): boolean;
    function LoopValue: TObject;
    function getLoopIndex: integer;
    constructor Create(D:TDataset);
    // function FieldByName():TDataset
    function ValS(const field:string ): string;
    function ValI(const field:string): integer;
    property S[const field:string]: string read ValS; default;
    property I[const field:string]: Integer read ValI;
  end;



  IDatasetStore = interface ['{38504B62-0B7E-4245-9787-9DCBA7FC22BB}']
    function Dataset(const name:string): TDatasetCacheTS;
    function CheckDataset(const name:string;DatasetClass: TDatasetCacheClass=nil): TDatasetCacheTS;
    procedure Add(const name: string;ADataset:TDatasetCacheTS);
  end;

  TDatasetStore= class (TInterfacedObject,IDatasetStore)
  protected
    FDatasets: TStringlist;
  public
    constructor Create();
    destructor Destroy; override;
    function Dataset(const name:string): TDatasetCacheTS;
    function CheckDataset(const name:string; DatasetClass: TDatasetCacheClass=nil): TDatasetCacheTS;
    procedure Add(const name: string;ADataset:TDatasetCacheTS);
    function Info(): string;
  end;

var
  g_cacheStore: TDatasetStore;
  g_cacheStoreSlave: boolean;             // in the dll we use isapi instance

implementation

uses NsUtil,variants,TypInfo;

{ TDatasetCacheRow }

{pre: index where to add field, -1 means at end
post: }
procedure TDatasetCacheRow.AddField(atIndex: Integer=-1);
var i,n: integer;
begin

  n:= high(FieldIndexes);
  SetLength(FieldIndexes,n+2);

  // now move rest on up
  if atIndex=-1 then begin
    FieldIndexes[n+1]:= FieldIndexes[n];
  end
  else begin
    for i:= n downto atIndex  do
      FieldIndexes[i+1]:= FieldIndexes[i];
    if atIndex=0 then FieldIndexes[atIndex]:= 0
    else FieldIndexes[atIndex]:= FieldIndexes[atIndex+1];  // length 0
  end;

end;

{pre:
post: data of matching fields is copied }
procedure TDatasetCacheRow.Assign(Source: TPersistent);
var i,j: integer; sameFormat: boolean; Src:TDatasetCacheRow;
begin

  if Source.InheritsFrom(TDatasetCacheRow) then begin
    Src:= TDatasetCacheRow(Source);
    sameFormat:= not (Self.InheritsFrom(TDatasetCacheRowOpt) xor Source.InheritsFrom(TDatasetCacheRowOpt));
    if sameFormat then sameFormat:= FOwner.Fields.Text=Src.FOwner.Fields.Text;

    if sameFormat then begin
      // copy internal data when same fields, dataformat
      FieldIndexes:= Src.FieldIndexes;
      data:= Src.data;
    end
    else begin
      for i:= 0 to FOwner.Fields.Count-1 do
        if Src.FOwner.Fields.Find(FOwner.Fields[i],j) then
          setField( Integer(FOwner.Fields.Objects[i]), Src.getField(j) );
    end;
    // if Source.InheritsFrom(TDatasetCacheRowOpt) then begin
    // end;
  end
  else raise Exception.Create('Invalid Assign, expected TDatasetCacheRow');

end;

constructor TDatasetCacheRow.Create(aOwner: TDatasetCacheTS);
var i,n:integer;
begin

  FOwner:= aOwner;
  if(FOwner<>nil) then begin
    n:= FOwner.Fields.count;
    SetLength(FieldIndexes,n);
    dec(n);
    for i := 0 to n do FieldIndexes[i]:= 0;
  end;

end;

destructor TDatasetCacheRow.Destroy;
begin
  if FIndexes<>nil then FIndexes.Free;
  inherited;
end;

//  Some debug function
function TDatasetCacheRow.GetAsString: string;
var i,n:integer;
begin
  n:= FOwner.Fields.Count-1;
  for i := 0 to n do
    Result:= Result + ',' + FOwner.Fields[i]+':'+GetS(FOwner.Fields[i]);
  Result:= copy(Result,2);
end;

function TDatasetCacheRow.getField(const index: integer): string;
begin
  //if index> high(indexes)
  if index=0 then Result:= copy(data,0,FieldIndexes[index]-0)
  else Result:= copy(data,FieldIndexes[index-1]+1,FieldIndexes[index]-FieldIndexes[index-1]);
end;

function TDatasetCacheRow.getFieldI(const index: integer): integer;
begin
  Result:= StrToIntDef( getField(index), 0 );
end;

function TDatasetCacheRow.getI(field: string): integer;
var i: integer;
begin
  if not FOwner.Fields.Find(field,i) then
  begin
    Result:= GetPropValue(Self,field,False);
    // raise Exception.Create('Field not found: '+field);
    exit;
  end;
  i:= Integer(FOwner.Fields.Objects[i]);
  Result:= getFieldI(i);
end;

function TDatasetCacheRow.GetIndex(
  const IndexName: string; createIfNotExists:boolean): TDatasetCacheRowList;
begin
  if FIndexes=nil then begin FIndexes:= TDatasetCacheIndexes.Create(); FIndexes.OwnsObjects:= True; end;
  Result:= FIndexes.getIndex(IndexName,createIfNotExists);
end;

function TDatasetCacheRow.GetS(field: string): string;
var i: integer;
begin
  if not FOwner.Fields.Find(field,i) then begin
    Result:= GetPropValue(self,field,True);
    // if MethodName(field) then
    // else raise Exception.Create('Field not found: '+field);
    exit;
  end;
  i:= Integer(FOwner.Fields.Objects[i]);
  Result:= getField(i);
end;

// works only for 1 index field
function TDatasetCacheRow.id: integer;
begin
  Result:= GetI(FOwner.FcsvIndexFields)
end;

function TDatasetCacheRow.Info: string;
var i:integer;
begin
  Result:= FOwner.FCacheName+' ( ';
  for i:= 0 to Fowner.Fields.Count-1 do begin
    Result:= Result+Fowner.Fields[i]+':';
    try
      Result:= Result+' '+S[Fowner.Fields[i]];
    except
      Result:= '!fld error!';
    end;
  end;
  Result:= Result+')';
end;

procedure TDatasetCacheRow.setData(const D: TDataset);
var i,l,p: integer; lFieldIndexes: TIntArray; ldata: string;
begin

  if D=nil then begin
    // empty dataset
    data:= '';
    SetLength(FieldIndexes,FOwner.Fields.Count);
    for i := 0 to High(FieldIndexes) do
      FieldIndexes[i]:= 0;
    // FillChar( FieldIndexes,size,#0);
    Exit;
  end;

  // normal fill
  ldata:= '';
  l:= D.Fields.Count;
  SetLength(lFieldIndexes,l);
  p:= 0;
  for i:= 0 to l-1 do begin
    ldata:= ldata+ D.Fields[i].AsString;
    p:= p+Length(D.Fields[i].AsString);
    lFieldIndexes[i]:= p;
  end;
  // critical moment when writing in object:
  data:= ldata;
  FieldIndexes:= lFieldIndexes;
end;

procedure TDatasetCacheRow.setField(const index: integer; const Value: string);
var p1,p2, l,ld: integer; s:string;
begin

  // current length
  if index=0 then begin p1:= 0; l:= FieldIndexes[index]-0 end
  else begin p1:= FieldIndexes[index-1]; l:= FieldIndexes[index]-FieldIndexes[index-1]; end;
  // delta length
  ld:= Length(Value)-l;
  if ld<>0 then begin
    data:= copy(data,1,p1)+Value+copy(data,FieldIndexes[index]+1,length(data));
    // update indexes
    for l:= index to High(FieldIndexes) do
      FieldIndexes[l]:= FieldIndexes[l ]+ ld;
  end
  else move(Value[1],data[1+p1],l);

end;

{ only for data fields }
procedure TDatasetCacheRow.SetI(field: string; const Value: integer);
var i: integer;
begin
  if not FOwner.Fields.Find(field,i) then raise Exception.Create('Field not found: '+field);
  i:= Integer(FOwner.Fields.Objects[i]);
  setField(i,IntToStr(Value));
end;

procedure TDatasetCacheRow.SetS(field: string; const Value: string);
var i: integer;
begin
  if not FOwner.Fields.Find(field,i) then raise Exception.Create('Field not found: '+field);
  i:= Integer(FOwner.Fields.Objects[i]);
  setField(i,Value);
end;

{ TDatasetCache }

{pre:
post: dynamicly add an field to the cache}
function TDatasetCacheTS.AddField(Fieldname: string;
  FieldType: TFieldType): integer;
var i,n:integer;
begin

  n:= Fields.Count;
  Fields.AddObject(FieldName,TObject(n)); // at end of field index
  SetLength( FieldsType, n+1 );
  FieldsType[n]:= FieldType;
  Fields.Sorted:= True;

  // Now also add index to rows
  for i:=0 to Rows.Count-1 do begin
    TDatasetCacheRow(Rows.Objects[i]).AddField(-1);
  end;

end;

function TDatasetCacheTS.AddRow(const key: string): TDatasetCacheRow;
begin

  Result:= ClassRow.Create(self);
  Rows.AddObject(key,Result);

end;

{pre:
post: copy objects, do not share objects(use index) }
procedure TDatasetCacheTS.Assign(aObject: TObject);
var i:integer; fieldsExists:boolean; lst: TStringlist; lRow: TDatasetCacheRow;
begin
  if aObject.InheritsFrom(TDatasetCacheTS) then begin
    if (ClassRow=TDatasetCacheRow) or (ClassRow=TDatasetCacheRowOpt) then
      ClassRow:= TDatasetCacheTS(aObject).ClassRow;
    // copy fielddefs
    fieldsExists:= Fields.count<>0;
    if not fieldsExists then begin
      Fields.assign(TDatasetCacheTS(aObject).fields);
      FieldsType:= TDatasetCacheTS(aObject).FieldsType; // tbv integer opt
    end;
    // copy objects
    lst:= TDatasetCacheTS(aObject).Rows;
    for I := 0 to lst.Count-1 do begin
      lRow:= AddRow(lst[i]);
      if fieldsExists then
        lRow.Assign(TPersistent(lst.Objects[i]))
      else begin
        // make sure internal data format is same
        lRow.data:= TDatasetCacheRow(lst.Objects[i]).data;
        lRow.FieldIndexes:= TDatasetCacheRow(lst.Objects[i]).FieldIndexes;
      end;
    end;

  end;
end;

procedure TDatasetCacheTS.Assign(const D: TDataset; const csvIndexFields: string; const AllRecords:boolean=True);
var i,s,p: integer; bEof: boolean; Fld:string; Fidx: TField; Idxs: array of TField;
begin
  // D.Last;
  // Rows.Capacity:= D.recordcount;
  // D.First;
  FcsvIndexFields:= csvIndexFields;
  Fields.Sorted:= False;
  Fields.Clear;
  SetLength(FieldsType,D.Fields.Count);
  for i:= 0 to D.Fields.Count-1 do begin
    Fields.AddObject(D.Fields[i].FieldName,TObject(i));
    FieldsType[i]:= D.Fields[i].DataType;
  end;
  Fields.Sorted:= True;

  Rows.Clear;
  bEof:= D.IsEmpty;

  i:= SepFieldsCount(csvIndexFields,',');
  if i<=1 then begin
    //
    Fidx:= D.FindField(csvIndexFields);
    if (Fidx=nil) and (csvIndexFields<>'') then
      raise Exception.Create('Index fields '+csvIndexFields+' not found in '+FCacheName);
    while not bEof do begin
      if shouldAddRecord(D) then begin
        FLastRow:= ClassRow{TDatasetCacheRow}.Create(Self);
        FLastRow.setData(D);
        if Fidx=nil then Rows.AddObject('',FLastRow)
        else Rows.AddObject(Fidx.AsString,FLastRow);
        if not AllRecords then break;
      end;
      D.Next;
      bEof:= D.Eof;
    end;
    if not D.IsEmpty and (Fidx<>nil) and (FLastId<Fidx.AsString) then FLastId:= Fidx.AsString;

    if i<>0 then Rows.Sorted:= True;

  end
  else begin
    //
    SetLength(Idxs,i);
    p:= 1; i:= 0;
    // determine
    Fld:= SepFieldsNext( csvIndexFields, p, ',');
    while Fld <>'' do begin
      Idxs[i]:= D.FindField(Fld);
      if(Idxs[i]=nil) then raise Exception.Create('Error Field not found: '+Fld);
      Fld:= SepFieldsNext( csvIndexFields, p, ',');
      inc(i);
    end;
    // now loop through records
    while not bEof do begin
      if shouldAddRecord(D) then begin
        FLastRow:= ClassRow{TDatasetCacheRow}.Create(self);
        FLastRow.setData(D);
        Fld:= Idxs[0].AsString;
        for I := 1 to High(Idxs) do
          Fld:= Fld+'_'+Idxs[i].AsString;
        Rows.AddObject(Fld,FLastRow);
        if not AllRecords then break;
      end;
      D.Next;
      bEof:= D.Eof;
    end;
    if not D.IsEmpty and (FLastId<Fld) then FLastId:= Fld;
    Rows.Sorted:= True;
  end;

end;

procedure TDatasetCacheTS.AssignFields(aObject: TObject);
begin

  if aObject.InheritsFrom(TDatasetCacheTS) then begin
    Fields.Assign( TDatasetCacheTS(aObject).Fields );
    FieldsType:= TDatasetCacheTS(aObject).FieldsType;
    Sql:= TDatasetCacheTS(aObject).Sql;
  end;


end;

procedure TDatasetCacheTS.Clear;
begin
  Rows.Clear;
end;

function TDatasetCacheTS.Count: Integer;
begin
  Result:= Rows.Count;
end;

constructor TDatasetCacheTS.Create(NamedCache:string);
begin
  FCacheName:= NamedCache;
  Fields:= TStringlist.Create;
  Rows:= TStringlist.Create;
  { TODO -ons -cfix : Version 2.3.1.33 -for pdf reporter temporary needed }
  Rows.Duplicates:= dupAccept;
  Rows.OwnsObjects:= True;
  ClassRow:= TDatasetCacheRow;
  // LoopIndex:= -1;
  // FirstLoop:= True;
  if FCacheName<>'' then
    g_cacheStore.Add(FCacheName,self);

end;

destructor TDatasetCacheTS.Destroy;
begin
  Rows.Clear;
  Rows.Free;
  Fields.Free;
  if FIndexes<>nil then FIndexes.Free;
  inherited;
end;



function TDatasetCacheTS.FindRow(const value: integer): integer;
begin
  if not Rows.Sorted then begin
    Rows.Sorted:= True;
  end;
  if not Rows.Find(IntToStr(value),Result) then Result:= -1;
end;

function TDatasetCacheTS.GetIndex(const IndexName: string;
  createIfNotExists: boolean): TDatasetCacheRowList;
begin
  if FIndexes=nil then begin FIndexes:= TDatasetCacheIndexes.Create(); FIndexes.Sorted:= True; FIndexes.OwnsObjects:= True; end;
  Result:= FIndexes.getIndex(IndexName,createIfNotExists);
end;

function TDatasetCacheTS.FindRow(const value: array of variant): integer;
var i: integer; key: string;
begin
  key:= VarToStr(value[0]);
   for i := 1 to High(value) do
     key:= Key+'_'+VarToStr(value[i]);
  Result:= FindRow(key);

end;

function TDatasetCacheTS.FindRow(const value: string): integer;
begin
  if not Rows.Sorted then begin
    Rows.Sorted:= True;
  end;
  Result:= 0;
  if not Rows.Find(value,Result) then Result:= -1;
end;

function TDatasetCacheTS.Loop(var aloopindex:integer): boolean;
begin
{
  if aloopindex=-1 then begin
    // use global - not threadsafe
    // if FirstLoop then FirstLoop:= False
    inc(Loopindex);
    if Loopindex>=Rows.Count then begin Loopindex:=-1; Result:= False; end
    else Result:= True;
  end
  else
}
  begin
    // use thread given loop var
    inc(aloopindex);
    if aloopindex>=Rows.Count then begin aloopindex:=-1; Result:= False; end
    else Result:= True;
  end;
end;

function TDatasetCacheTS.Obj: TDatasetCacheTS;
begin
  Result:= Self;
end;

{pre: a record is found in dataset
post: decide if record should be added to set }
function TDatasetCacheTS.shouldAddRecord(D:TDataset): boolean;
begin
  Result:= True;
end;

{pre:
post: add mising records, does not update records }
function TDatasetCacheTS.Refresh(const D: TDataset; const UpdateRecords: boolean ): boolean;
var i,s,p,r: integer; Fld:string; Fidx: TField; Idxs: array of TField;
begin

  // ' cache '+classname+', records updated: '+IntToStr(D.recordcount);
  Fields.Sorted:= False;
  Fields.Clear;
  SetLength(FieldsType,D.Fields.Count);
  for i := 0 to D.Fields.Count-1 do begin
    Fields.AddObject(D.Fields[i].FieldName,TObject(i));
    FieldsType[i]:= D.Fields[i].DataType;
  end;
  Fields.Sorted:= True;

  i:= SepFieldsCount(FcsvIndexFields,',');
  if i=1 then begin
    // single field index
    Fidx:= D.FindField(FcsvIndexFields);
    while not D.eof do begin
      if shouldAddRecord(D) then begin
        r:= FindRow(Fidx.AsString);
        if (r<>-1) then begin
          if not UpdateRecords then continue;
          FLastRow:= getRow(r);
          FLastRow.setData(D);
        end
        else begin

          FLastRow:= ClassRow{TDatasetCacheRow}.Create(Self);
          FLastRow.setData(D);
          // critical: add object to list,
          if Fidx=nil then Rows.AddObject('',FLastRow)
          else Rows.AddObject(Fidx.AsString,FLastRow);
        end;
      end;
      D.Next; // else break;
    end;
    if not D.IsEmpty and (Fidx<>nil) and (FLastId<Fidx.AsString) then FLastId:= Fidx.AsString
  end
  else begin
    // multiple field index
    SetLength(Idxs,i);
    p:= 1; i:= 0;
    Fld:= SepFieldsNext( csvIndexFields, p, ',');
    while Fld <>'' do begin
      Idxs[i]:= D.FindField(Fld);
      if(Idxs[i]=nil) then raise Exception.Create('Error Field not found: '+Fld);
      Fld:= SepFieldsNext( csvIndexFields, p, ',');
      inc(i);
    end;
    while not D.eof do begin
      if shouldAddRecord(D) then begin
        Fld:= Idxs[0].AsString;
        for I := 1 to High(Idxs) do
          Fld:= Fld+'_'+Idxs[i].AsString;
        r:= FindRow(Fld);
        if (r<>-1) then begin
          if not UpdateRecords then continue;
          FLastRow:= getRow(r);
          FLastRow.setData(D);
        end
        else begin
          FLastRow:= ClassRow{TDatasetCacheRow}.Create(self);
          FLastRow.setData(D);
          Rows.AddObject(Fld,FLastRow);
        end;
      end;
      D.Next;
    end;
    if not D.IsEmpty and (FLastId<Fld) then FLastId:= Fld;
  end;
  Rows.Sorted:= True;

end;

function TDatasetCacheTS.Row4Id(const value: integer): TDatasetCacheRow;
var i: integer;
begin
  if not Rows.Sorted then begin
    Rows.Sorted:= True;
  end;
  if not Rows.Find(IntToStr(value),i) then Result:= nil
  else begin
    Result:= TDatasetCacheRow(Rows.Objects[i]);
  end;
end;

function TDatasetCacheTS.Row4Id(const value: string): TDatasetCacheRow;
var i: integer;
begin
  if not Rows.Sorted then begin
    Rows.Sorted:= True;
  end;
  if not Rows.Find(value,i) then Result:= nil
  else begin
    Result:= TDatasetCacheRow(Rows.Objects[i]);
  end;
end;


function TDatasetCacheTS.getIterator(indexname: string; csvIndexFields:string=''; ignoreempty:boolean=True ): TCacheRowsIterator;
var i,f,p,p1: integer; s:string; Src:TStringList; flds: array of string; fldspad: array of integer;
//var lst: TDatasetCacheRowList;
begin

  if indexname<>'' then begin
    Src:= getIndex(indexname,False);
    if Src=nil then raise Exception.Create('Error index '+indexname+' not found in '+classname);
    end
  else Src:= Rows;
  Result:= TCacheRowsIterator.Create;
  Result.FLoopIndex:= -1;
  if csvIndexFields='' then begin
    Result.FList.Assign(Src);
    if (Result.FList.count>0) and (Result.FList[1]<>'') then
      Result.FList.Sorted:= True;
    Exit;
  end;

  // interpred Index Format
  f:= SepFieldsCount(csvIndexFields,',');
  setLength(flds,f); p:= 1;
  setLength(fldspad,f);
  for i := 0 to f-1 do begin
    flds[i]:= SepFieldsNext( csvIndexFields, p, ',');
    p1:= pos(':',flds[i]);
    if(p1>0) then fldspad[i]:= StrToInt( copy(flds[i],p1+1,99) )
    else fldspad[i]:= 0;
  end;

  for i := 0 to Src.Count-1 do
  begin
    if fldspad[0]=0 then s:= TDatasetCacheRow(Src.Objects[i]).S[flds[0]]
    else s:= Zero( TDatasetCacheRow(Src.Objects[i]).S[flds[0]], fldspad[0] );
    if ignoreempty and ((s='') or (s='0')) then continue;

    for f := 1 to High(flds) do
      if fldspad[f]=0 then  s:=s+'_'+TDatasetCacheRow(Src.Objects[i]).S[flds[f]]
      else s:=s+'_'+Zero( TDatasetCacheRow(Src.Objects[i]).S[flds[f]], fldspad[f] ) ;
    Result.FList.AddObject(s,Src.Objects[i]);
  end;
  Result.FList.Sorted:= True;

end;

function TDatasetCacheTS.getRow(const rowIdx: integer): TDatasetCacheRow;
begin
  //if rowIdx=-1 then Result:= TDatasetCacheRow(Rows.Objects[Loopindex])else
  Result:= TDatasetCacheRow(Rows.Objects[rowIdx]);
end;

// info about data etc
function TDatasetCacheTS.Info: string;
var i,n:integer; res: string;
begin

  res:= 'Dataset '+FCacheName+', ';
  res:= res + IntToStr(Rows.Count)+' rows, ';
  n:= 0;
  for I := 0 to Rows.Count-1 do
    n:= n + Length( TDatasetCacheRow(Rows.Objects[i]).data );

  res:= res + IntToStr(round(n/1000))+'kb \n';
  Result:= res;

end;

function TDatasetCacheTS.Val(const field: string; row: integer): string;
var lrow: TDatasetCacheRow; col: integer;
begin
  //if row=-1 then row:= Loopindex;
  lrow:= TDatasetCacheRow(Rows.Objects[row]);
  if Fields.Find(field,col) then Result:= lrow.getField( Integer(Fields.Objects[col]) )
  else raise Exception.Create('Field not found :'+field);
end;

{function TDatasetCache.ValI(const field: string ): integer;
begin
  Result:= StrToIntDef( Val(field,-1 ),0);
end;

function TDatasetCache.ValS(const field: string): string;
begin
  Result:= Val(field,-1);
end;
}

function TDatasetCacheTS.Val(const col:integer; row:integer): string;
var lrow: TDatasetCacheRow;
begin
  //if row=-1 then row:= Loopindex;
  lrow:= TDatasetCacheRow(Rows.Objects[row]);
  Result:= lrow.getField(col);
end;

{ TDatasetCacheRowOpt }

function TDatasetCacheRowOpt_.getField(const index: integer): string;
var i:integer;
begin
  //Result:= inherited getField(index);
  if FOwner.FieldsType[index]=ftInteger then begin
    // convert binairy format
    if Result='' then begin // lege string, leeg veld
      Result:= '0';
      exit;
    end;
    Move(Result[1],i,SizeOf(i));
    Result:= IntToStr(i);
  end

end;

function TDatasetCacheRowOpt_.getFieldI(const index: integer): integer;
var s:string; // i:integer;
begin
  s:= inherited getField(index);
  if (FOwner.FieldsType[index]=ftInteger) then begin
    // convert binairy format
    if s='' then Result:= 0
    else Move(s[1],Result,SizeOf(Result));
    //Result:= IntToStr(i);
  end
  else Result:= StrToIntDef(s,0);
end;

procedure TDatasetCacheRowOpt_.setData(const D: TDataset);
type tparc= packed array[1..2] of char;
var i,l,p,v: integer; ar: tparc; lFieldIndexes: TIntArray; ldata:string;
begin
  //inherited setData(D);

  if D=nil then begin
    // empty dataset
    data:= '';
    SetLength(FieldIndexes,FOwner.Fields.Count);
    for i := 0 to High(FieldIndexes) do
      FieldIndexes[i]:= 0;
    // FillChar( FieldIndexes,size,#0);
    Exit;
  end;


  l:= D.Fields.Count;
  ldata:= '';
  SetLength(lFieldIndexes,l);
  p:= 0;
  for i:= 0 to l-1 do begin
    if D.Fields[i].DataType=ftInteger then begin
      ar:= tparc(D.Fields[i].AsInteger);
      ldata:= ldata+ '  ';
      v:= D.Fields[i].AsInteger;
      move(v,ldata[p+1],SizeOf(v));
      inc(p,2);
    end
    else begin
      ldata:= ldata+ Trim(D.Fields[i].AsString);
      p:= p+Length(Trim(D.Fields[i].AsString));
    end;
    lFieldIndexes[i]:= p;
  end;
  // critical moment when writing in object:
  data:= ldata;
  FieldIndexes:= lFieldIndexes;

end;

procedure TDatasetCacheRowOpt_.setField(const index: integer;const Value: string);
var p1,p2, l,ld,v: integer; s:string;
begin

  // current length
  if index=0 then begin p1:= 0; l:= FieldIndexes[index]-0 end
  else begin p1:= FieldIndexes[index-1]; l:= FieldIndexes[index]-FieldIndexes[index-1]; end;

  // delta length
  if FOwner.FieldsType[index]=ftInteger then begin
    ld:= 2-l;
    if(ld<>0) then
      data:= copy(data,1,p1)+'  '+copy(data,FieldIndexes[index]+1,length(data));
    //data:= s;
    v:= StrToIntDef(Value,0);
    if  p1>length(data) then
      raise Exception.Create('Error setField');
    move(v,data[1+p1],SizeOf(v));
  end
  else begin
    ld:= Length(Value)-l;
    data:= copy(data,1,p1)+Value+copy(data,FieldIndexes[index]+1,length(data));
    //data:= s;
  end;

  // update indexes
  if ld<>0 then
  for l:= index to High(FieldIndexes) do
    FieldIndexes[l]:= FieldIndexes[l ]+ ld;

end;

{ TDatasetStore }

procedure TDatasetStore.Add(const name: string;ADataset: TDatasetCacheTS);
var i:integer;
begin
  if FDatasets.Find(name,i) then exit; //raise Exception.Create('Error Dataset '+name+' allready in list');
  FDatasets.AddObject(name,ADataset);
end;

function TDatasetStore.CheckDataset(const name: string; DatasetClass: TDatasetCacheClass=nil ): TDatasetCacheTS;
var i:integer;
begin
  if FDatasets.Find(name,i) then begin
    Result:= TDatasetCacheTS(FDatasets.Objects[i]);
    // raise Exception.Create('Error Dataset '+name+' allready in list');
  end
  else begin
    if DatasetClass<>nil then Result:= DatasetClass.Create('')
    else Result:= TDatasetCacheTS.Create('');
    Result.FCacheName:= name;
    FDatasets.AddObject(name,Result);
  end;
end;

constructor TDatasetStore.Create;
begin
  FDatasets:= TStringlist.Create();
  FDatasets.Sorted:= True;
  // FDatasets.Options
  FDatasets.OwnsObjects:= True;
end;

function TDatasetStore.Dataset(const name: string): TDatasetCacheTS;
var i:integer;
begin
  if FDatasets.Find(name,i) then Result:= TDatasetCacheTS(FDatasets.Objects[i])
  else raise Exception.Create('Error DatasetCache '+name+' not found');
  //Result:= nil;
end;

destructor TDatasetStore.Destroy;
begin
  FDatasets.Clear;
  FDatasets.Free;
  FDatasets:= nil;
  inherited;
end;

function TDatasetStore.Info: string;
var i: integer;
begin
  Result:= '';
  for I := 0 to FDatasets.Count-1 do
    Result:= Result + TDatasetCacheTS(FDatasets.Objects[i]).Info() + '\n';

end;

{ TDatasetCacheIndexes }

function TDatasetCacheIndexes.getIndex(indexname: string; createIfNotExists: boolean): TDatasetCacheRowList;
var i:integer;
begin
  if not Find(indexname,i) then begin
     if createIfNotExists then
     begin
       Result:= TDatasetCacheRowList.Create();
       AddObject(indexname,Result);
       Sorted:= True;
       Result.ignoreempty:= True;
     end
     else Result:= nil
  end
  else Result:= TDatasetCacheRowList(Objects[i]);
end;

function TDatasetCacheIndexes.getIterator(indexname: string): TCacheRowsIterator;
var lst: TDatasetCacheRowList;
begin
  lst:= getIndex(indexname);
  Result:= TCacheRowsIterator.Create;
  Result.LoopIndex:= -1;
  TStringlist(Result).Assign(lst);
end;

{ TDatasetCacheRowList }

procedure TDatasetCacheRowList.AddRow(obj: TDatasetCacheRow; keyvalue: string; Check4Existing:boolean=False );
var i:integer; var lrow: TDatasetCacheRow;
begin
  if Check4Existing then begin
    Sorted:= True;
    if Find(keyvalue,i) then begin
      if Row[i]=nil then nsutil.ShowMsg(self,'Error!');
      Objects[i]:= obj;
      end
    else AddObject(keyvalue,obj);
  end
  else
    AddObject(keyvalue,obj);
end;

{pre: field1:3,field2[:2]
post: }
procedure TDatasetCacheRowList.Assign(SourceObj: TObject; csvIndexFields:string);
var i,f,p,p1: integer; s:string; Src:TStringlist; flds: array of string; fldspad: array of integer;
begin
  i:= -1;
  if SourceObj.InheritsFrom(TDatasetCacheRowList) then begin
    Src:= TDatasetCacheRowList(SourceObj);
  end
  else if SourceObj.InheritsFrom(TDatasetCacheTS) then begin
    Src:= TDatasetCacheTS(SourceObj).Rows;
  end;

  Sorted:= False;
  Clear;
  if csvIndexFields='' then begin
    inherited Assign(src);
    exit;
  end;

  // interpred Index Format
  f:= SepFieldsCount(csvIndexFields,',');
  setLength(flds,f); p:= 1;
  setLength(fldspad,f);
  for i := 0 to f-1 do begin
    flds[i]:= SepFieldsNext( csvIndexFields, p, ',');
    p1:= pos(':',flds[i]);
    if(p1>0) then begin
      fldspad[i]:= StrToInt( copy(flds[i],p1+1,99) );
      flds[i]:= copy(flds[i],1,p1-1);
    end
    else fldspad[i]:= 0;
  end;

  if Duplicates<>dupAccept then Sorted:= True;
  for i := 0 to Src.Count-1 do
  begin
    if fldspad[0]=0 then s:= TDatasetCacheRow(Src.Objects[i]).S[flds[0]]
    else s:= Zero( TDatasetCacheRow(Src.Objects[i]).S[flds[0]], fldspad[0], '0' );
    if ignoreempty and ((s='') or (s='0')) then continue;

    for f := 1 to High(flds) do
      if fldspad[f]=0 then  s:=s+'_'+TDatasetCacheRow(Src.Objects[i]).S[flds[f]]
      else s:=s+'_'+Zero( TDatasetCacheRow(Src.Objects[i]).S[flds[f]], fldspad[f], '0' ) ;
    AddObject(s,Src.Objects[i]);
  end;
  Sorted:= True;

end;


procedure TDatasetCacheRowList.Changed;
begin
  inherited Changed;
  FLastFind:= nil;
end;

constructor TDatasetCacheRowList.Create;
begin
  inherited Create;
  FLastFindCache:= True;
end;

function TDatasetCacheRowList.getRow(index: integer): TDatasetCacheRow;
begin
  Result:= TDatasetCacheRow( Objects[index] )
end;

function TDatasetCacheRowList.Loop(var i:Integer): boolean;
begin
  inc(i);
  if i>=count then begin Result:= False; end
  else Result:= True;
end;

function TDatasetCacheRowList.Row4key(key: string): TDatasetCacheRow;
var i:integer;
begin
  if FLastFindCache and (FLastFind<>nil) and (FLastFindKey=key) then begin
    Result:= FLastFind;
    Exit;
  end;
  Sorted:= True;
  if Find(key,i) then begin
    Result:= TDatasetCacheRow(Objects[i]);
    if FLastFindCache then begin
      FLastFindKey:= key;
      FLastFind:= Result;
    end;
  end
  else Result:= nil;
end;

{ TDatasetCacheNTS }

constructor TDatasetCacheNTS.Create(NamedCache:string);
begin
  inherited Create(NamedCache);
  LoopIndex:= -1;
end;

function TDatasetCacheNTS.Loop: boolean;
begin
  Result:= inherited Loop(loopIndex);
end;

function TDatasetCacheNTS.ValI(const field: string): integer;
begin
  if LoopIndex=-1 then LoopIndex:= 0;
  Result:= getRow(LoopIndex).I[field];
end;

function TDatasetCacheNTS.ValS(const field: string): string;
begin
  if LoopIndex=-1 then LoopIndex:= 0;
  Result:= getRow(LoopIndex).S[field];
end;

{ TDatasetFacade }

function TDatasetFacade.Count: Integer;
begin
  Result:= FD.RecordCount;
end;

constructor TDatasetFacade.Create(D: TDataset);
begin
  FD:= D;
end;

function TDatasetFacade.Eof: boolean;
begin
  Result:= FD.Eof;
end;

function TDatasetFacade.getLoopIndex: integer;
begin
  assert(true,'to be implemented');
  Result:= -1;
end;

function TDatasetFacade.Loop: boolean;
begin
  Result:= not FD.Eof;
end;

function TDatasetFacade.LoopValue: TObject;
begin
  Result:= FD;
end;

procedure TDatasetFacade.Reset;
begin
  FD.First;
end;

function TDatasetFacade.ValI(const field: string): integer;
begin
  FD.FieldByName(field).AsInteger;
end;

function TDatasetFacade.ValS(const field: string): string;
begin
  FD.FieldByName(field).AsString;
end;

{ TCacheRowsIterator }

function TCacheRowsIterator.Count: integer;
begin
  Result:= FList.Count
end;

constructor TCacheRowsIterator.Create;
begin
  FList:= TStringlist.Create;
  { TODO -ons -cfix : Fix: e want to keep double entries in our list }
  FList.Duplicates:= dupAccept;
  LoopIndex:= -1;
end;

destructor TCacheRowsIterator.Destroy;
begin
  FList.Free;
  inherited;
end;

function TCacheRowsIterator.Eof: boolean;
begin
  Result:= FLoopIndex>=count;
end;

function TCacheRowsIterator.getLoopIndex: Integer;
begin
  Result:= FLoopIndex;
end;

function TCacheRowsIterator.Loop: boolean;
begin
  inc(FLoopIndex);
  if LoopIndex>=Flist.count then begin Result:= False; end
  else Result:= True;
  // Result:= Loop(LoopIndex);
end;

function TCacheRowsIterator.LoopValue: TObject;
begin
  Result:= FList.Objects[LoopIndex];
end;

procedure TCacheRowsIterator.Reset;
begin
  LoopIndex:= -1;
end;

procedure TCacheRowsIterator.setLoopIndex(Value: integer);
begin
  FLoopIndex:= Value;
end;

function TCacheRowsIterator.ValI(const field: string): integer;
begin
  Result:= TDatasetCacheRow(LoopValue).getI(field);
end;

function TCacheRowsIterator.ValS(const field: string): string;
begin
  Result:= TDatasetCacheRow(LoopValue).getS(field);
end;

initialization

  g_cacheStore:= TDatasetStore.Create;
  g_cacheStoreSlave:= False;
finalization

  if (g_cacheStore<>nil) and not g_cacheStoreSlave then try g_cacheStore.Free except end;

end.
